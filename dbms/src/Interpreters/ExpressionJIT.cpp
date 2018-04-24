#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ExpressionJIT.h>

#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Mangler.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Verifier.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h>
#include <llvm/ExecutionEngine/Orc/NullResolver.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Target/TargetMachine.h>

#include <stdexcept>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct LLVMContext::Data
{
    llvm::LLVMContext context;
    std::shared_ptr<llvm::Module> module;
    std::unique_ptr<llvm::TargetMachine> machine;
    llvm::orc::RTDyldObjectLinkingLayer objectLayer;
    llvm::orc::IRCompileLayer<decltype(objectLayer), llvm::orc::SimpleCompiler> compileLayer;
    llvm::DataLayout layout;
    llvm::IRBuilder<> builder;

    Data()
        : module(std::make_shared<llvm::Module>("jit", context))
        , machine(llvm::EngineBuilder().selectTarget())
        , objectLayer([]() { return std::make_shared<llvm::SectionMemoryManager>(); })
        , compileLayer(objectLayer, llvm::orc::SimpleCompiler(*machine))
        , layout(machine->createDataLayout())
        , builder(context)
    {
        module->setDataLayout(layout);
        module->setTargetTriple(machine->getTargetTriple().getTriple());
        // TODO: throw in some optimization & verification layers
    }

    llvm::Type * toNativeType(const DataTypePtr & type)
    {
        // LLVM doesn't have unsigned types, it has unsigned instructions.
        if (type->equals(DataTypeInt8{}) || type->equals(DataTypeUInt8{}))
            return builder.getInt8Ty();
        if (type->equals(DataTypeInt16{}) || type->equals(DataTypeUInt16{}))
            return builder.getInt16Ty();
        if (type->equals(DataTypeInt32{}) || type->equals(DataTypeUInt32{}))
            return builder.getInt32Ty();
        if (type->equals(DataTypeInt64{}) || type->equals(DataTypeUInt64{}))
            return builder.getInt64Ty();
        if (type->equals(DataTypeFloat32{}))
            return builder.getFloatTy();
        if (type->equals(DataTypeFloat64{}))
            return builder.getDoubleTy();
        return nullptr;
    }

    LLVMCompiledFunction * lookup(const std::string& name)
    {
        std::string mangledName;
        llvm::raw_string_ostream mangledNameStream(mangledName);
        llvm::Mangler::getNameWithPrefix(mangledNameStream, name, layout);
        // why is `findSymbol` not const? we may never know.
        return reinterpret_cast<LLVMCompiledFunction *>(compileLayer.findSymbol(mangledNameStream.str(), false).getAddress().get());
    }
};

LLVMContext::LLVMContext()
    : shared(std::make_shared<LLVMContext::Data>())
{}

void LLVMContext::finalize()
{
    shared->module->print(llvm::errs(), nullptr, false, true);
    if (shared->module->size())
        llvm::cantFail(shared->compileLayer.addModule(shared->module, std::make_shared<llvm::orc::NullResolver>()));
    shared->module->print(llvm::errs(), nullptr, false, true);
}

bool LLVMContext::isCompilable(const IFunctionBase& function) const
{
    if (!function.isCompilable() || !shared->toNativeType(function.getReturnType()))
        return false;
    for (const auto & type : function.getArgumentTypes())
        if (!shared->toNativeType(type))
            return false;
    return true;
}

LLVMPreparedFunction::LLVMPreparedFunction(LLVMContext context, std::shared_ptr<const IFunctionBase> parent)
    : parent(parent), context(context), function(context->lookup(parent->getName()))
{}

LLVMFunction::LLVMFunction(ExpressionActions::Actions actions_, LLVMContext context)
    : actions(std::move(actions_)), context(context)
{
    std::unordered_set<std::string> seen;
    for (const auto & action : actions)
        seen.insert(action.result_name);
    for (const auto & action : actions)
    {
        const auto & names = action.argument_names;
        const auto & types = action.function->getArgumentTypes();
        for (size_t i = 0; i < names.size(); i++)
        {
            if (seen.emplace(names[i]).second)
            {
                arg_names.push_back(names[i]);
                arg_types.push_back(types[i]);
            }
        }
    }

    llvm::FunctionType * func_type = llvm::FunctionType::get(context->builder.getVoidTy(), {
        llvm::PointerType::getUnqual(llvm::PointerType::getUnqual(context->builder.getVoidTy())),
        llvm::PointerType::getUnqual(context->builder.getInt8Ty()),
        llvm::PointerType::getUnqual(context->toNativeType(actions.back().function->getReturnType())),
        context->builder.getIntNTy(sizeof(size_t) * 8),
    }, /*isVarArg=*/false);
    auto * func = llvm::Function::Create(func_type, llvm::Function::ExternalLinkage, actions.back().result_name, context->module.get());
    auto args = func->args().begin();
    llvm::Value * inputs = &*args++; // void** - tuple of columns, each a contiguous data block
    llvm::Value * consts = &*args++; // char* - for each column, 0 if it is full, 1 if it points to a single constant value
    llvm::Value * output = &*args++; // void* - space for the result
    llvm::Value * counter = &*args++; // size_t - number of entries to read from non-const values and write to output

    auto * entry = llvm::BasicBlock::Create(context->context, "entry", func);
    context->builder.SetInsertPoint(entry);

    std::vector<llvm::Value *> inputs_v(arg_types.size());
    std::vector<llvm::Value *> deltas_v(arg_types.size());
    for (size_t i = 0; i < arg_types.size(); i++)
    {
        if (i != 0)
        {
            inputs = context->builder.CreateConstGEP1_32(inputs, 1);
            consts = context->builder.CreateConstGEP1_32(consts, 1);
        }
        auto * type = llvm::PointerType::getUnqual(context->toNativeType(arg_types[i]));
        auto * step = context->builder.CreateICmpEQ(context->builder.CreateLoad(consts), llvm::ConstantInt::get(context->builder.getInt8Ty(), 0));
        inputs_v[i] = context->builder.CreatePointerCast(context->builder.CreateLoad(inputs), type);
        deltas_v[i] = context->builder.CreateZExt(step, context->builder.getInt32Ty());
    }

    auto * loop = llvm::BasicBlock::Create(context->context, "loop", func);
    context->builder.CreateBr(loop); // assume nonzero initial value in `counter`
    context->builder.SetInsertPoint(loop);

    std::unordered_map<std::string, llvm::Value *> by_name;
    std::vector<llvm::PHINode *> phi(inputs_v.size());
    for (size_t i = 0; i < inputs_v.size(); i++)
    {
        phi[i] = context->builder.CreatePHI(inputs_v[i]->getType(), 2);
        phi[i]->addIncoming(inputs_v[i], entry);
    }
    auto * output_phi = context->builder.CreatePHI(output->getType(), 2);
    auto * counter_phi = context->builder.CreatePHI(counter->getType(), 2);
    output_phi->addIncoming(output, entry);
    counter_phi->addIncoming(counter, entry);

    for (size_t i = 0; i < phi.size(); i++)
        if (!by_name.emplace(arg_names[i], context->builder.CreateLoad(phi[i])).second)
            throw Exception("duplicate input column name", ErrorCodes::LOGICAL_ERROR);
    for (const auto & action : actions)
    {
        ValuePlaceholders action_input;
        action_input.reserve(action.argument_names.size());
        for (const auto & name : action.argument_names)
            action_input.push_back(by_name.at(name));
        if (!by_name.emplace(action.result_name, action.function->compile(context->builder, action_input)).second)
            throw Exception("duplicate action result name", ErrorCodes::LOGICAL_ERROR);
    }
    context->builder.CreateStore(by_name.at(actions.back().result_name), output_phi);

    for (size_t i = 0; i < phi.size(); i++)
        phi[i]->addIncoming(context->builder.CreateGEP(phi[i], deltas_v[i]), loop);
    output_phi->addIncoming(context->builder.CreateConstGEP1_32(output_phi, 1), loop);
    counter_phi->addIncoming(context->builder.CreateSub(counter_phi, llvm::ConstantInt::get(counter_phi->getType(), 1)), loop);

    auto * end = llvm::BasicBlock::Create(context->context, "end", func);
    context->builder.CreateCondBr(context->builder.CreateICmpNE(counter_phi, llvm::ConstantInt::get(counter_phi->getType(), 1)), loop, end);
    context->builder.SetInsertPoint(end);
    context->builder.CreateRetVoid();
}

}


namespace
{

struct LLVMTargetInitializer
{
    LLVMTargetInitializer()
    {
        llvm::InitializeNativeTarget();
        llvm::InitializeNativeTargetAsmPrinter();
    }
};

}

static LLVMTargetInitializer llvmInitializer;
