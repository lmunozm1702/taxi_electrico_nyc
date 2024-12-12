function laCajaDePandora(numero) {
    if (numero % 2 === 0) {
        return numero.toString(2);
    } else {
        return numero.toString(16);
    }
}

function joseQuispe() {
    return {
        nombre: "Jose Quispe",
        edad: 23,
        nacionalidad: "Argentino"
    };
}
