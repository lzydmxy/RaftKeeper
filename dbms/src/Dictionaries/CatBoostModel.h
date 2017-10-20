#pragma once
#include <Interpreters/IExternalLoadable.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

/// CatBoost wrapper interface functions.
struct CatBoostWrapperApi;
class CatBoostWrapperApiProvider
{
public:
    virtual ~CatBoostWrapperApiProvider() = default;
    virtual const CatBoostWrapperApi & getApi() const = 0;
};

/// CatBoost model interface.
class ICatBoostModel
{
public:
    virtual ~ICatBoostModel() = default;
    /// Evaluate model. Use first `float_features_count` columns as float features,
    /// the others `cat_features_count` as categorical features.
    virtual ColumnPtr eval(const Columns & columns, size_t float_features_count, size_t cat_features_count) const = 0;
};

/// General ML model evaluator interface.
class IModel : public IExternalLoadable
{
public:
    virtual ColumnPtr evaluate(const Columns & columns) const = 0;
};

class CatBoostModel : public IModel
{
public:
    CatBoostModel(const std::string & name, const std::string & model_path,
                  const std::string & lib_path, const ExternalLoadableLifetime & lifetime,
                  size_t float_features_count, size_t cat_features_count);

    ColumnPtr evaluate(const Columns & columns) const override;

    size_t getFloatFeaturesCount() const;
    size_t getCatFeaturesCount() const;

    /// IExternalLoadable interface.

    const ExternalLoadableLifetime & getLifetime() const override;

    std::string getName() const override { return name; }

    bool supportUpdates() const override { return true; }

    bool isModified() const override;

    std::unique_ptr<IExternalLoadable> cloneObject() const override;

    std::exception_ptr getCreationException() const override { return creation_exception; }

private:
    std::string name;
    std::string model_path;
    std::string lib_path;
    ExternalLoadableLifetime lifetime;
    std::exception_ptr creation_exception;
    std::shared_ptr<CatBoostWrapperApiProvider> api_provider;
    const CatBoostWrapperApi * api;

    std::unique_ptr<ICatBoostModel> model;

    size_t float_features_count;
    size_t cat_features_count;

    void init(const std::string & lib_path);
};

}
