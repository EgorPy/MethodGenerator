import { BACKEND_URL } from "/static/config.js"

async function logout(event) {
    if (event) {
        event.preventDefault()
    }

    const response = await fetch(`${BACKEND_URL}/auth/logout`, {
        credentials: "include"
    })

    window.location.href = "/register"
}

let logout_button = document.querySelector("#logout_button")
let logout_section = document.querySelector("#logout_section")
let sure_logout_button = document.querySelector("#sure_logout_button")
let cancel_logout_button = document.querySelector("#cancel_logout_button")
let body = document.querySelector("body")
logout_button.onclick = () => nigger()
//logout_section.onclick = () => anti_nigger()
cancel_logout_button.onclick = () => anti_nigger()
sure_logout_button.onclick = () => logout()

function nigger() {
    logout_section.style.display = "flex"
    body.style.position = "fixed"
}

function anti_nigger() {
    logout_section.style.display = "none"
    body.style.position = "static"
}
