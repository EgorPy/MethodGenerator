def redirect_on_success(path: str):
    def decorator(func):
        setattr(func, "__redirect_on_success__", path)
        return func

    return decorator
