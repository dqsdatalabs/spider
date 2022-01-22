# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from time import time
from random import randint
import json
from python_spiders.helper import extract_rent_currency, property_type_lookup, format_date, extract_location_from_coordinates


class ChiccaImmobiliare_PySpider_italy_it(scrapy.Spider):
    name = "chicca_immobiliare_it"
    allowed_domains = ["chiccaimmobiliare.com", "widget.getrix.it"]
    start_urls = ['http://www.chiccaimmobiliare.com/it/annunci-immobiliari?fromSearch=true&Contratto=A&Categoria=1&Provincia=&Comune=&Quartiere=&Zona=&PrezzoMin=&PrezzoMax=&SuperficieMin=&SuperficieMax=&Locali=']
    country = 'italy'
    locale = 'it'
    external_source = f"ChiccaImmobiliare_PySpider_{country}_{locale}"
    execution_type = 'testing'
    thousand_separator='.'
    scale_separator=','
    position = 1

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    months = {
        'gennaio': "January",
        'febbraio': "February",
        'marzo': "March",
        'aprile': "April",
        'maggio': "May",
        'giugno': "June",
        'luglio': "July",
        'agosto': "August",
        'settembre': "September",
        'ottobre': "October",
        'novembre': "November",
        'dicembre': "December",
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.get_first_token, headers=self.headers)

    def get_first_token(self, response):
        parsed_url = urlparse("https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?apikey=6BD7AAAA-7B2A-4149-AD86-47532B15A55F&querystring=%3FfromSearch%3Dtrue%26Contratto%3DA%26Categoria%3D1%26Provincia%3D%26Comune%3D%26Quartiere%3D%26Zona%3D%26PrezzoMin%3D%26PrezzoMax%3D%26SuperficieMin%3D%26SuperficieMax%3D%26Locali%3D&clientId=&clientClassName=&numeroColonne=2&lingua=it&numeroRisultati=20&urlPaginaRisultati=%2Fit%2Fannunci-immobiliari&urlPaginaAnnuncio=%2Fit%2Fimmobile%2F%7Bcategoria%7D%2F%7Bcontratto%7D%2F%7Btipologia%7D%2F%7Bcomune%7D-%7Bsiglaprovincia%7D%2F%7Bidimmobile%7D&urlPaginaCantiere=%2Fit%2Fcantiere%2F%7Bidcantiere%7D&mostraTitoli=true&mostraZonaQuartiere=true&mostraDescrizione=true&mostraGallery=true&carousel=true&mostraDettagli=true&mostraBottoni=true&mostraAnnunciSalvati=true&mostraIcone=true&mostraPrivacy=true&urlPrivacy=%2Fit%2Fprivacy&startRisultati=-1&RewriteProvincia=&RewriteComune=&RewriteQuartiere=&RewriteTipologia=&tolerancePrezzo=0&toleranceSuperficie=0&toleranceLocali=false&annunciSalvati=&_=1637583660")
        url_parts = list(parsed_url)
        query = dict(parse_qsl(url_parts[4]))
        apikey = response.css("input[type=hidden]::attr('value')").get()
        timestamp = str(int(time()))
        query.update({
            'apikey': apikey,
            '_': timestamp
        })
        url_parts[4] = urlencode(query)
        url = urlunparse(url_parts)
        yield scrapy.Request(url, callback=self.parse, meta={ 'apikey': apikey, 'timestamp': timestamp }, headers=self.headers)

    def parse(self, response):
        data = json.loads(response.body)
        data = json.loads(data)["html"]
        sel = Selector(text=data, type="html")
        for listing in sel.css(".gx-risultato"):
            rent, currency = extract_rent_currency(listing.css(".gx-prezzo span+span::text").get(), self.country, ChiccaImmobiliare_PySpider_italy_it)
            if rent and currency:
                room_count = int(listing.css(".gx-caratteristiche-lista-2 span+span::text").get())
                square_meters = int(listing.css(".gx-caratteristiche-lista-3 span+span::text").get().split(" ")[0])
                external_id = listing.css(".gx-caratteristiche-lista-4 span+span::text").get()
                path_query = listing.xpath("a/@href").get()
                url = "http://" + self.allowed_domains[0] + path_query
                yield scrapy.Request(url, callback=self.get_second_token, headers=self.headers)

    def get_second_token(self, response):
        parsed_url = urlparse("https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/dettaglio?IDImmobile=0&runtimeId=gxAnnuncio-1637683979416491909&clientId=&clientClassName=&lingua=it&mostraTitolo=true&mostraBottoneStampa=true&mostraBottoniSocial=true&mostraBottoneInviaAmico=true&mostraDescrizione=true&mostraBottoneLeggiTutto=true&mostraBottoneCantiere=true&mostraMappa=true&mostraDettagli=true&mostraBottoneDettagli=false&mostraIconaAsta=false&mostraGallery=true&carousel=true&thumbnails=bottom&mostraVideo=true&mostraPlanimetrie=true&mostraVirtualTour=true&mostraRichiestaInfo=true&mostraPrivacy=true")
        url_parts = list(parsed_url)
        query = dict(parse_qsl(url_parts[4]))
        apikey = response.css("input[type=hidden]::attr('value')").get()
        timestamp = str(int(time()*1000))
        path = urlparse(response.url).path
        runtimeId = "gxAnnuncio-" + timestamp + str(randint(100000, 999999))
        query.update({
            'apikey': apikey.encode(),
            '_': timestamp.encode(),
            'pathName': path.encode(),
            'runtimeId': runtimeId.encode(),
            'urlPrivacy': b"/it/privacy",
            'urlPaginaCantiere': b"/it/cantiere/{idcantiere}",
            'urlPaginaAnnuncio': b"/it/immobile/{categoria}/{contratto}/{tipologia}/{comune}-{siglaprovincia}/{idimmobile}",
        })
        url_parts[4] = urlencode(query)
        url = urlunparse(url_parts)
        yield scrapy.Request(
            url,
            callback=self.populate_item,
            meta={ 'page_url': response.url, 'apikey': apikey, 'timestamp': timestamp },
            headers=self.headers,
        )

    def populate_item(self, response):
        data = json.loads(response.body)
        data = json.loads(data)["html"]
        sel = Selector(text=data, type="html")

        rent, currency = extract_rent_currency(sel.css(".gx-prezzo span+span::text").get(), self.country, ChiccaImmobiliare_PySpider_italy_it)
        external_id = sel.css(".gx-caratteristiche-lista-4 span+span::text").get()
        description = sel.css(".gx-printable::text").extract()
        latitude = sel.css("#gx-map-canvas::attr('data-lat')").get()
        longitude = sel.css("#gx-map-canvas::attr('data-lng')").get()
        zipcode = city = address = None
        if longitude and latitude:
            zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        images = []
        for image in sel.css(".gx-printable-img::attr('style')").extract():
            left = image.find("(") + 1
            right = image.find(")")
            images.append(image[left:right])

        floor = energy_label = room_count = bathroom_count = property_type = square_meters = parking = balcony = None
        for row in sel.css(".gx-row-details"):
            label = row.css("label::text").get().lower()
            if "livelli" in label:
                floor = row.css("span::text").get()
            elif label == "classe energetica:":
                energy_label = row.css("span::text").get()
            elif "bagni" in label:
                bathroom_count = row.css("span::text").get()
            elif "tipologia" in label:
                property_type = property_type_lookup.get(row.css("span::text").get())
            elif "superficie" in label:
                square_meters = int(row.css("span::text").get())
            elif "box" in label:
                parking = row.css("span::text").get() != "assente"
            elif "camere" in label:
                room_count = int(row.css("span::text").get())
            elif "balconi" in label:
                balcony = row.css("span::text").get() == "Sì"

        available_date = pets_allowed = washing_machine = None
        for line in map(str.lower, description):
            if "disponibile dal" in line:
                left = line.find("disponibile dal ") + len("disponibile dal ")
                right = left + line[left:].find(".")
                parsed_date = line[left:right]
                for month in self.months:
                    if month in parsed_date:
                        parsed_date = parsed_date.replace(month, self.months[month])
                available_date = format_date(parsed_date, "%d %B %Y")
            if "no animali" in line:
                pets_allowed = False
            if "lavatrice" in line:
                if "no lavatrice" in line:
                    washing_machine = False
                else:
                    washing_machine = True

        item_loader = ListingLoader(response=response)
        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.meta.get("page_url"))
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("title", sel.css("li>strong::text").get())
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("zipcode", zipcode)

        item_loader.add_value("floor", floor)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("parking", parking)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("balcony", balcony)

        item_loader.add_value("available_date", available_date)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("images", images)
        item_loader.add_value("description", "\r\n".join(description))


        details_url = urlparse("https://widget.getrix.it/api/2.0/sitowebagenzia/widgets/anagrafica-agenzia?lingua=it&idAgenzia=0")
        url_parts = list(details_url)
        query = dict(parse_qsl(url_parts[4]))
        query.update({
            'apikey': response.meta.get("apikey"),
            '_': response.meta.get("timestamp"),
        })
        url_parts[4] = urlencode(query)
        url = urlunparse(url_parts)
        self.position += 1
        yield scrapy.Request(
            url,
            callback=self.get_details,
            meta={ "item_loader": item_loader, 'city': city, 'address': address },
            headers=self.headers
        )

    def get_details(self, response):

        data = json.loads(response.body)
        data = json.loads(data)["html"]

        city = response.meta.get("city")
        address = response.meta.get("address")
        if not city:
            city = data.get("Provincia")
        if not address:
            address = city

        item_loader = response.meta.get("item_loader")
        item_loader.add_value("city", city)
        item_loader.add_value("address", address)
        item_loader.add_value("landlord_name", data.get("Agenzia"))
        item_loader.add_value("landlord_email", data.get("Email"))
        item_loader.add_value("landlord_phone", data.get("Telefono"))

        yield item_loader.load_item()
