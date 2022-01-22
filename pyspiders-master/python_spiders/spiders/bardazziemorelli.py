# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from scrapy.selector import Selector
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from time import time
from random import randint
import json
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_rent_currency, property_type_lookup, extract_number_only, extract_location_from_coordinates


class Bardazziemorelli_PySpider_italy_it(scrapy.Spider):
    name = "bardazziemorelli"
    start_urls = ['https://www.bardazziemorelli.it/it/annunci-immobiliari?fromSearch=true&Contratto=A&Categoria=1&Provincia=&Comune=&Quartiere=&Zona=&PrezzoMin=&PrezzoMax=&SuperficieMin=&SuperficieMax=&Locali=&Vani=&Bagni=&Riferimento=&ClasseEnergetica=']
    allowed_domains = ["bardazziemorelli.it", "widget.getrix.it"]
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = 'testing'
    thousand_separator='.'
    scale_separator=','
    position = 1

    company_name = "Bardazzi E Morelli S.r.l."

    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    cities = ["Milano", "Genova"]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.get_first_token, headers=self.headers)

    def get_first_token(self, response):
        parsed_url = urlparse("https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/lista?clientId=&clientClassName=&numeroColonne=3&lingua=it&mostraTitoli=true&mostraZonaQuartiere=true&mostraDescrizione=true&mostraGallery=true&carousel=true&mostraDettagli=true&mostraBottoni=true&mostraAnnunciSalvati=true&mostraIcone=true&mostraPrivacy=true&startRisultati=-1&RewriteProvincia=&RewriteComune=&RewriteQuartiere=&RewriteTipologia=&tolerancePrezzo=0&toleranceSuperficie=0&toleranceLocali=false&annunciSalvati=")
        url_parts = list(parsed_url)
        query = dict(parse_qsl(url_parts[4]))
        apikey = response.css("input[type=hidden]::attr('value')").get()
        timestamp = str(int(time()))
        query.update({
            'querystring': b"?fromSearch=true&Contratto=A&Categoria=1&Provincia=&Comune=&Quartiere=&Zona=&PrezzoMin=&PrezzoMax=&SuperficieMin=&SuperficieMax=",
            'urlPaginaRisultati': b"/it/annunci-immobiliari",
            'urlPaginaAnnuncio': b"/it/immobile/{categoria}/{contratto}/{tipologia}/{comune}-{siglaprovincia}/{idimmobile}",
            'urlPaginaCantiere': b"/it/cantiere/{idcantiere}",
            'urlPrivacy': b"/it/privacy",
            'numeroRisultati': 10,
            'apikey': apikey,
            '_': timestamp,
        })
        url_parts[4] = urlencode(query)
        url = urlunparse(url_parts)
        yield scrapy.Request(url, callback=self.parse, meta={ 'apikey': apikey, 'timestamp': timestamp }, headers=self.headers)

    def parse(self, response, **kwargs):
        data = json.loads(response.body)
        data = json.loads(data)["html"]
        sel = Selector(text=data, type="html")

        for path_query in sel.css(".gx-risultato > a::attr('href')").extract():
            url = "http://" + self.allowed_domains[0] + path_query
            yield scrapy.Request(url, callback=self.get_second_token, headers=self.headers)

    def get_second_token(self, response):
        parsed_url = urlparse("https://widget.getrix.it/api/2.0/sitowebagenzia/annunci/dettaglio?IDImmobile=0&clientId=&clientClassName=&lingua=it&mostraTitolo=true&mostraBottoneStampa=true&mostraBottoniSocial=true&mostraBottoneInviaAmico=true&mostraDescrizione=true&mostraBottoneLeggiTutto=true&mostraBottoneCantiere=true&mostraMappa=true&mostraDettagli=true&mostraBottoneDettagli=false&mostraIconaAsta=false&mostraGallery=true&carousel=true&thumbnails=bottom&mostraVideo=true&mostraPlanimetrie=true&mostraVirtualTour=true&mostraRichiestaInfo=true&mostraPrivacy=true")
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
        yield scrapy.Request(url, callback=self.populate_item, headers=self.headers, meta={ 'page_url': response.url })

    def populate_item(self, response):
        data = json.loads(response.body)
        data = json.loads(data)["html"]
        sel = Selector(text=data, type="html")

        title = sel.css(".gx-scheda-testo h1::text").get()
        rent, currency = extract_rent_currency(sel.css(".gx-prezzo span+span::text").get(), self.country, Bardazziemorelli_PySpider_italy_it)
        room_count = sel.css(".gx-caratteristiche-lista-2 span+span::text").get()
        latitude = sel.css("#gx-map-canvas::attr('data-lat')").get()
        longitude = sel.css("#gx-map-canvas::attr('data-lng')").get()

        zipcode = city = address = None
        if latitude and longitude:
            zipcode, city, address = extract_location_from_coordinates(float(longitude), float(latitude))

        floor = energy_label = bathroom_count = property_type = square_meters = parking = balcony = utilities = elevator = None
        for row in sel.css(".gx-row-details"):
            label = row.css("label::text").get().lower()
            if "piano" in label:
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
            elif "ascensori" in label:
                elevator = row.css("span::text").get() != "0"
            elif "spese" in label:
                utilities = int(extract_number_only(row.css("span::text").get().replace(",00", "")))

        images = []
        for image in sel.css(".gx-printable-img::attr('style')").extract():
            left = image.find("(") + 1
            right = image.find(")")
            images.append(image[left:right])

        description = sel.css(".gx-annuncio-descrizione p::text").get()

        furnished = None
        if "arredato" in description:
            furnished = True

        camere_index = description.find("camere")
        if camere_index != -1:
            parsed_count = int(extract_number_only(description[camere_index-3:camere_index]))
            if parsed_count != 0:
                room_count = parsed_count

        landlord_email = landlord_name = landlord_phone = None
        last = description.find(" Per maggiori informazioni")
        if last != -1:
            contact_details = description[last:].lower()
            description = description[:last]
            landlord_name = self.company_name
            tel_index = contact_details.find("tel.") + len("tel.")
            landlord_phone = contact_details[tel_index:tel_index+11].strip()
            for detail in contact_details.split(" "):
                if "@" in detail:
                    landlord_email = detail

        if not landlord_phone:
            landlord_phone = sel.css(".gx-caratteristiche-lista-4 span+span::text").get()

        if not city:
            for c in self.cities:
                if c in title:
                    city = c

        if not address:
            address = city

        item_loader = ListingLoader(response=response)

        item_loader.add_value("position", self.position)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.meta.get("page_url"))
        item_loader.add_value("title", title)

        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("address", address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("floor", floor)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)


        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("balcony", balcony)

        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("utilities", utilities)
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("images", images)
        item_loader.add_value("description", description)

        self.position += 1
        yield item_loader.load_item()
