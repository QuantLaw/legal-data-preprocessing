def get_docparts_with_p(soup):
    return [
        soup.titelzeile,
        soup.leitsatz,
        soup.sonstosatz,
        soup.tenor,
        soup.tatbestand,
        soup.entscheidungsgruende,
        soup.gruende,
        soup.sonstlt,
        soup.abwmeinung,
    ]
