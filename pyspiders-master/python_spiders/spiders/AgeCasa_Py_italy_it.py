# -*- coding: utf-8 -*-
# Author: Omar Ibrahim

import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import property_type_lookup, remove_white_spaces, extract_number_only, extract_rent_currency
import urllib.parse as urlparse


class AgeCasa_PySpider_italy_it(scrapy.Spider):
    name = "age_casa_com"
    allowed_domains = ["agecasa.com"]
    start_urls = [
        "https://www.agecasa.com/risultati-ricerca?filter_immo_agenzia_id=&filter_immo_regione=&filter_immo_provincia=&filter_immo_comune=&filter_immo_zona=&filter_immo_tipologia_contratto_id=2&filter_immo_tipologia_id=&filter_immo_prezzo=&filter_immo_locali_totali=&filter_immo_rif_tecnico="
    ]
    execution_type = "testing"
    country = "italy"
    locale = "it"
    thousand_separator = '.'
    scale_separator = ','
    position = 1
    page = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        for property in response.css(".list-group a"):
            property_type = remove_white_spaces(property.css("#tipo_immo div::text").get()).capitalize()
            is_valid_property = property_type_lookup.get(property_type, '') in ["apartment", "house", "room", "student_apartment", "studio"]
            is_for_rent = remove_white_spaces(property.css("#contratto div::text").get()).lower() == "affitto"
            if is_valid_property and is_for_rent:
                url = property.css("::attr('href')").get()
                yield scrapy.Request(url, callback=self.populate_item)

        if response.css(".pagination li.active+li>a::attr('link')").get():
            self.page += 1
            url_parts = list(urlparse.urlparse(response.url))
            query = dict(urlparse.parse_qsl(url_parts[4]))
            query.update({ "pagina_schede": self.page })
            url_parts[4] = urlparse.urlencode(query)
            yield scrapy.Request(urlparse.urlunparse(url_parts), self.parse)

    def populate_item(self, response):
        description_items = response.xpath("//div[contains(@id, 'scheda_descrizione')]/text()").extract()
        room_count = int(extract_number_only(response.css(".cp.camere > div::text").get()))

        required = {
            "external_link": response.url,
            "external_source": f"AgeCasa_PySpider_{self.country}_{self.locale}",
            "property_type": property_type_lookup.get(remove_white_spaces(response.css(".cp.tipologia > div::text").get()), ""),
            "square_meters": int(response.css(".cp.superficie > div::text").get()),
            "room_count": room_count if room_count > 0 else None,
            "currency": "EUR",
        }
        required["rent"], _ = extract_rent_currency(
            response.css(".costo > .lista_campo::text").get(), self.country, AgeCasa_PySpider_italy_it
        )

        item_loader = ListingLoader(response=response)
        for key, val in required.items():
            if not val:
                return
            item_loader.add_value(key, val)

        bathroom_count = int(extract_number_only(response.css(".cp.bagni > div::text").get()))
        if bathroom_count > 0:
            item_loader.add_value("bathroom_count", int(response.css(".cp.bagni > div::text").get()))

        item_loader.add_value("external_id", remove_white_spaces(response.css(".rif_annucio div::text").get()))
        item_loader.add_value("floor", remove_white_spaces(response.css(".cp.piano > div::text").get()))
        item_loader.add_value("elevator", False if remove_white_spaces(response.css(".cp.ascensore > div::text").get()).lower() == "no" else True)
        item_loader.add_value("furnished", True if remove_white_spaces(response.css(".cp.arredamento > div::text").get()).lower() == "arredato" else False)
        item_loader.add_value("utilities", int(response.css(".cp.speseCondom > div::text").get()))
        item_loader.add_value("title", remove_white_spaces(response.css(".Titolo > div::text").get()))

        cleaned_description = []
        for line in description_items:
            if "tel." in line or "Chiamaci " in line or "Contattaci" in line:
                continue
            if "ageCASA" in line:
                continue
            cleaned_description.append(remove_white_spaces(line))
        item_loader.add_value("description", "\r\n".join(cleaned_description))

        landlord_phone = response.xpath("//div[contains(text(), '+39')]/text()").extract().pop()
        if landlord_phone and landlord_phone.startswith("+39"):
            item_loader.add_value("landlord_phone", landlord_phone)

        longitude = response.css(".longit .lista_campo::text").get()
        latitude = response.css(".latitu .lista_campo::text").get()

        if not (longitude == "0" and latitude == "0"):
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        city = remove_white_spaces(response.css(".comune > .lista_campo::text").get())
        item_loader.add_value("city", city)
        item_loader.add_value("address", ",".join([city]))

        images = response.css("img::attr('src')").extract()
        excluded_image = response.css(".imgAgenzia img::attr('src')").get()
        item_loader.add_value("images", [image for image in images if image.endswith(".jpg") and image != excluded_image])

        floor_plan = response.xpath("//div[contains(@id, 'scheda_campo_personalizzato_')]/text()").extract().pop()
        if floor_plan and floor_plan.endswith(".jpg"):
            item_loader.add_value("floor_plan_images", [floor_plan])

        energy_label = remove_white_spaces(response.css(".classeE > div::text").get())
        item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("position", self.position)
        self.position += 1

        yield item_loader.load_item()
