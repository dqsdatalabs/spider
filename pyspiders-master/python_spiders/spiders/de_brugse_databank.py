# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json
import unicodedata
import dateparser
import re

class MySpider(Spider):
    name = "de_brugse_databank"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source='Debrugsedatabank_PySpider_belgium_nl'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.de-brugse-databank.be/Producten/Tehuur/Studentenkamer/lijstmain.htm",
                "property_type" : "student_apartment"
            },
            {
                "url" : "https://www.de-brugse-databank.be/Producten/Tehuur/Huis/lijstmain.htm",
                "property_type" : "house"
            },
            {
                "url" : "https://www.de-brugse-databank.be/Producten/Tehuur/Villa/lijstmain.htm",
                "property_type" : "house"
            },
            {
                "url" : "https://www.de-brugse-databank.be/Producten/Tehuur/Appartement/lijstmain.htm",
                "property_type" : "apartment"
            },
        ]# LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})
    
    # 1. FOLLOWING LEVEL 1
    def parse(self, response):

        for item in response.xpath(
            "//article//div[@class='folio-info']/a[1]/@href"
        ).extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Debrugsedatabank_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//div[@class='col-lg-9 col-md-9 col-sm-9']/h2//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        title = response.xpath("//title/text()").get()
        if "assistentiewoning" in title.lower():
            item_loader.add_value("property_type", "house")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))

        desc = "".join(response.xpath("//div[@class='col-lg-7 col-md-7 col-sm-8']//p/text()").extract())
        item_loader.add_value(
            "description", desc
        )

        price = response.xpath(
            "//div[@class='col-lg-3 col-md-3 col-sm-3']//h2/text()"
        ).extract_first()
        if price:
            r = unicodedata.normalize("NFKD", price)
            item_loader.add_value("rent", r.split("€ ")[1].replace(" ", ""))
        item_loader.add_value("currency", "EUR")

        ref = response.xpath("//div[@class='col-lg-3 col-md-3 col-sm-3']//h4/text()").get()
        if ref:
            ref = ref.split(".")[1]
            item_loader.add_value("external_id", ref.strip())
        
        square = response.xpath(
            "//tr[./td[.='Bewoonbare opp. m²' or .='Opp.bebouwd m²' or .='Living opp. in m²']]/td[2]/text()"
        ).extract_first()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])
        else:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2|m^2)",desc.replace(",","."))
            if unit_pattern:
                item_loader.add_value("square_meters", unit_pattern[0][0])

        available_date = response.xpath(
            "//tr[./td[.='Datum vrij']]/td[2]/text()"
        ).get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d/%m/%Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        room = response.xpath("//tr[./td[.='Aantal slaapkamers']]/td[2]/text()").get()
        if room:
            item_loader.add_value("room_count", room)
        item_loader.add_xpath(
            "floor", "//tr[./td[text()='Verdieping']]/td[2]/text()"
        )

        item_loader.add_xpath("utilities", "//tr[./td[.='Kosten']]/td[2]/text()")

        terrace = response.xpath("//tr[./td[text()='Terras']]/td[2]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        terrace = response.xpath("//tr[./td[.='Gemeubeld']]/td/text()").get()
        if terrace:
            item_loader.add_value("furnished", True)

        terrace = response.xpath(
            "//tr[./td[text()='Type pand']]/td[.='Garage']"
        ).get()
        if terrace:
            item_loader.add_value("parking", True)
        else:
            if "garage" in desc or "Garage" in desc or "parkeren" in desc or "Parkeren" in desc:
                item_loader.add_value("parking", True)

        terrace = response.xpath("//tr[./td[text()='Lift']]/td/text()").get()
        if terrace:
            item_loader.add_value("elevator", True)
        else:
            if "lift" in desc or "Lift" in desc:
                item_loader.add_value("elevator", True)

        energy = response.xpath("//tr[./td[.='EPC kWh/m²']]/td[2]/text()").get()
        if energy:
            energy_label = energy.split(" ")[0].replace("uc:1131245","").strip()
            if energy_label.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy_label))

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@id='main-slider']/ul/li/img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        address = response.xpath(
            "//div[@class='col-lg-9 col-md-9 col-sm-9']//h4//text()"
        ).extract_first()
        if address:
            item_loader.add_value("address", address.strip().replace("|",","))
            city = address.split("(")[0].strip()
            zipcode = address.split("(")[1].split(")")[0]
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", city)

        phone = response.xpath(
            '//h1[@class="section-title"]/a[contains(@href, "tel:")]/@href'
        ).get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("tel:", ""))
        email = response.xpath(
            '//div[@class="contact-info"]//a[contains(@href, "mailto:")]/@href'
        ).get()
        if email:
            item_loader.add_value(
                "landlord_email", email.replace("mailto:", "")
            )
        item_loader.add_value("landlord_name", "De Brugse Databank")

        dishwasher = response.xpath(
            "//tr[./td[.='Keukentoestellen']]/td[.='Vaatwas']"
        ).get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        else:
            if "Vaatwas" in desc or "vaatwas" in desc:
                item_loader.add_value("dishwasher", True)
        
        if "wasmachine" in desc or "Wasmachine" in desc:
            item_loader.add_value("washing_machine", True)
        
        if "huisdier" in desc or "Huisdier" in desc:
            item_loader.add_value("pets_allowed", True)
        
        if "balkon" in desc or "Balkon" in desc:
            item_loader.add_value("balcony", True)
        
        if "zwembad" in desc or "Zwembad" in desc:
            item_loader.add_value("swimming_pool", True)

        item_loader.add_xpath("bathroom_count", "//tr[./td[contains(.,'Badkamers')]]/td[2]/text()")

        yield item_loader.load_item()
        
def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label
