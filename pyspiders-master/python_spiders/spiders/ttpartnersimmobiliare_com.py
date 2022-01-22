# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'ttpartnersimmobiliare_com'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Ttpartnersimmobiliare_PySpider_italy"
    start_urls = ['http://ttpartnersimmobiliare.com/intermedia.php']  # LEVEL 1

    formdata = {
        "ricerca": "si",
        "venditaAcquisto": "2",
        "categoria": "1",
        "comune": "0",
        "prezzoDa": "",
        "prezzoA": "",
        "superficieDa": "",
        "superficieA": "",
        "localiDa": "",
        "localiA": "",
        "riscaldamento": "0",
        "condizioniImmobile": "0",
        "bagni": "0",
        "button": "Effettua ricerca",
    }

    def start_requests(self):

        yield FormRequest(
            url=self.start_urls[0],
            callback=self.parse,
            formdata=self.formdata
        )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='box-immobili']/table"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath(
            "//table[@class='box-dettagli']//tr[td[contains(.,'Tipologia')]]/td[2]/text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value(
                "property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath(
            "//h2[contains(.,'Riferimento')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = response.xpath(
            "//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath(
            "//table[contains(@class,'box-dettagli')]//tr//td[contains(.,'Zona')]//following-sibling::td/text()").get()
        if address:
            item_loader.add_value("address", address)

        city = response.xpath(
            "//table[contains(@class,'box-dettagli')]//tr//td[contains(.,'Comune')]//following-sibling::td/text()").get()
        if city:
            item_loader.add_value("city", city)

        description = response.xpath(
            "//div[contains(@class,'box-immobili')]//p//text()").getall()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath(
            "//table[contains(@class,'box-dettagli')]//tr//td[contains(.,'Prezzo')]//following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€"))
        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath(
            "//table[contains(@class,'box-dettagli')]//tr//td[contains(.,'Superficie mq.')]//following-sibling::td/text()").getall()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        bathroom_count = response.xpath(
            "//table[contains(@class,'box-dettagli')]//tr//td[contains(.,'Numero Bagni')]//following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        room_count = response.xpath(
            "//td[@valign='top']//table[contains(@class,'box-dettagli')]//tr//td[contains(.,'Piani')]//following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

           
        energy_label = response.xpath(
            "//td[@valign='top']//table[contains(@class,'box-dettagli')]//tr//td[contains(.,'Classif.Energetica')]//following-sibling::td/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label) 
        balcony = response.xpath(
            "//table[contains(@class,'box-dettagli')]//tr//td[contains(.,'Numero Bagni')]//following-sibling::td/text()").get()
        if 'Si' in balcony:
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)

        terrace = response.xpath(
            "//table[contains(@class,'box-dettagli')]//tr//td[contains(.,'Terrazza abitabile')]//following-sibling::td/text()").get()
        if 'No' in terrace:
            item_loader.add_value("terrace", False)
        else:
            item_loader.add_value("terrace", True)

        parking = response.xpath(
            "//table[contains(@class,'box-dettagli')]//tr//td[contains(.,'garage')]//following-sibling::td/text()").get()
        if 'nessuno' in parking:
            item_loader.add_value("parking", False)
        else:
            item_loader.add_value("parking", True)

        images = [response.urljoin(x) for x in response.xpath(
            "//a[@class='fancy_a_el']//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Tt Partners Immobiliare")
        item_loader.add_value("landlord_phone", " +39 - 055 26 39 558")
        item_loader.add_value(
            "landlord_email", "info@ttpartnersimmobiliare.com")

        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("terratetto" in p_type_string.lower() or "casa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    else:
        return None
