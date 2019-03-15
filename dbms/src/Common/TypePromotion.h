#pragma once

#include <Common/typeid_cast.h>

namespace DB {

template <class Base>
class TypePromotion {
private:
    /// Need a helper-struct to fight the lack of the function-template partial specialization.
    template <class T, bool is_ref = std::is_reference_v<T>, bool is_const = std::is_const_v<T>>
    struct CastHelper;

    template <class T>
    struct CastHelper<T, true, false>
    {
        auto & value(Base * ptr) { return typeid_cast<T>(*ptr); }
    };

    template <class T>
    struct CastHelper<T, true, true>
    {
        auto & value(const Base * ptr) { return typeid_cast<T>(*ptr); }
    };

    template <class T>
    struct CastHelper<T, false, false>
    {
        auto * value(Base * ptr) { return typeid_cast<T *>(ptr); }
    };

    template <class T>
    struct CastHelper<T, false, true>
    {
        auto * value(const Base * ptr) { return typeid_cast<T *>(ptr); }
    };

public:
    template <class Derived>
    auto as() -> std::invoke_result_t<decltype(&CastHelper<Derived>::value), CastHelper<Derived>, Base *>
    {
        // TODO: if we do downcast to base type, then just return |this|.
        return CastHelper<Derived>().value(static_cast<Base *>(this));
    }

    template <class Derived>
    auto as() const -> std::invoke_result_t<decltype(&CastHelper<const Derived>::value), CastHelper<const Derived>, const Base *>
    {
        // TODO: if we do downcast to base type, then just return |this|.
        return CastHelper<const Derived>().value(static_cast<const Base *>(this));
    }
};

} // namespace DB
