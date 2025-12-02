export function login(service_name) {
    if (service_name != null && service_name !== "") {
        localStorage.setItem("service_name", service_name);
    }
    window.location.href = "/login"
}
