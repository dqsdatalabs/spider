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
    name = "desmet_poupeye_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source='Desmetpoupeye_PySpider_belgium_nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.desmet-poupeye.be/pages/selectie.asp?Aanbod=H&Type=1",
                "property_type" : "house"
            },
            {
                "url" : "http://www.desmet-poupeye.be/pages/selectie.asp?Aanbod=H&Type=2",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.desmet-poupeye.be/pages/selectie.asp?Aanbod=H&Type=3",
                "property_type" : "studio"
            },
            {
                "url" : "http://www.desmet-poupeye.be/pages/selectie.asp?Aanbod=H&Type=12",
                "property_type" : "student_apartment"
            },
        ]# LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//table//tr[@class='BodyText']"):
            follow_url = response.urljoin(item.xpath(".//a[@class='Title']/@href").get())
            address = item.xpath(".//span[@class='SubTitle']/text()").get()
            address = re.sub('\s{2,}', ' ', address)
            tehuur = item.xpath(".//img/@src[contains(.,'tehuur')]").get()
            if tehuur:
                try:              
                    price = "".join(item.xpath("./td[contains(.,'€')]/b/text()").get())
                    if price:
                        if "€" in price:
                            price = price.strip()
                except:
                    return
                
                yield Request(
                    follow_url, callback=self.populate_item, meta={"property_type": response.meta.get("property_type"), "address": address.strip(), "price" : price}
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Desmetpoupeye_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        title = response.xpath("(//span[@class='title2'])[1]//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_id", response.url.split("ID=")[1])
        
        desc = "".join(response.xpath("//table[@class='BodyText']//tr[./td[.='omschrijving :']]/td[2]/text()").getall())
        if desc:
            item_loader.add_value("description", desc)
        

        if "gemeubeld" in desc.lower():
            item_loader.add_value("furnished", True) 

        if "badkamer" in desc:
            bathroom = desc.split("badkamer")[0].split(",")[-1]
            if bathroom == " ":
                item_loader.add_value("bathroom_count","1")
        
        price = "".join(response.xpath("//tr/td/b[contains(.,'prijs')]/parent::td/following-sibling::td//text()").getall())
        if response.meta.get("price") and "€" in response.meta.get("price"):
            item_loader.add_value("rent_string", response.meta.get("price"))
        elif price:
            rent = price.split("€")[1].split(",")[0].strip()
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
        
        floor = response.xpath("//table[@class='BodyText']//tr[./td[contains(.,'gedetaileerde')]]/td[2]/b[contains(.,'verdieping')][last()]/text()").get()
        if floor:
            if " verdieping" in floor:
                floor = floor.split(" ")[0]
                item_loader.add_value("floor", floor)

        available_date = response.xpath("//tr/td/b[contains(.,'vrij')]/parent::td/following-sibling::td/text()[not(contains(.,'onmiddellijk'))]").get()
        date2=""
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
            item_loader.add_value("available_date", available_date)
        
        item_loader.add_value("address", response.meta.get("address"))
        address = response.meta.get("address").split("\u00a0")[-1].strip()
        if address:
            address = address.strip("-")
            if "-" in address:
                city = address.split("-")[-1].replace(")","").strip()
                item_loader.add_value("city", city.split(" ")[-1])
            else:
                item_loader.add_value("city", address)
        
        zipcode = response.meta.get("address").split(" - ")[-1].split(" ")[0].split("\u00a0")[0]
        if zipcode and zipcode.isdigit():
            item_loader.add_value("zipcode", zipcode)
        
        images = [
            response.urljoin(x)
            for x in response.xpath("//a[@target='Detail_iframe']/img/@src").extract()
        ]
        item_loader.add_value("images", images)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        square = response.xpath("//table[@class='BodyText']//tr//text()[contains(.,'Opp. : ')]").get()
        if square:
            square = square.split(":")[1].split(".")[1].strip().split("m²")[0]
            item_loader.add_value("square_meters", square)
        else:
            square = response.xpath("//table[@class='BodyText']//tr//text()[contains(.,'bruikbare vloeropperlakte:')]").get()
            if square:
                square = square.split(":")[1].split("m²")[0]
                item_loader.add_value("square_meters", square)
            else:
                square = response.xpath("//table[@class='BodyText']//tr[./td[contains(.,'gedetaileerde')]]/td[2]//text()[contains(.,'bewoonbare opp')]").get()
                if square:
                    square = square.split(":")[1].split("m²")[0]
                    item_loader.add_value("square_meters", square)
        
        bathroom = response.xpath("//table[@class='BodyText']//tr[./td[contains(.,'gedetaileerde')]]/td[2]//text()[contains(.,'badkamer')][last()]").get()
        if bathroom:
            bathroom = bathroom.split(":")[0].strip().split(" ")[-1].strip()
            item_loader.add_value("bathroom_count", bathroom)

        terrace = response.xpath("//table[@class='BodyText']//tr[./td[contains(.,'gedetaileerde')]]/td[2]//text()[contains(.,'terras')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            terrace = response.xpath("//table[@class='BodyText']//tr[./td[.='omschrijving :']]/td[2]/text()[contains(.,'terras')]").get()
            if terrace:
                item_loader.add_value("terrace", True)
        
        parking = response.xpath("//table[@class='BodyText']//tr[./td[contains(.,'gedetaileerde')]]/td[2]//text()[contains(.,'garage') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//table[@class='BodyText']//tr[./td[.='omschrijving :']]/td[2]/text()[contains(.,'garage') or contains(.,'parking')]").get()
            if parking:
                item_loader.add_value("parking", True)
        
        washing = response.xpath("//table[@class='BodyText']//tr[./td[contains(.,'gedetaileerde')]]/td[2]//text()[contains(.,'wasmachine')]").get()
        if washing:
            item_loader.add_value("washing_machine", True)
        else:
            washing = response.xpath("//table[@class='BodyText']//tr[./td[.='omschrijving :']]/td[2]/text()[contains(.,'wasmachine')]").get()
            if washing:
                item_loader.add_value("washing_machine", True)
        
        dishwasher = response.xpath("//table[@class='BodyText']//tr[./td[.='omschrijving :']]/td[2]/text()[contains(.,'vaatwas')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        elevator = response.xpath("//table[@class='BodyText']//tr[./td[.='omschrijving :']]/td[2]/text()[contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        else:
            elevator = response.xpath("//table[@class='BodyText']//tr[./td[contains(.,'gedetaileerde')]]/td[2]//text()[contains(.,'lift')]").get()
            if elevator:
                item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//table[@class='BodyText']//tr[./td[.='omschrijving :']]/td[2]/text()[contains(.,'balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        else:
            balcony = response.xpath("//table[@class='BodyText']//tr[./td[contains(.,'gedetaileerde')]]/td[2]//text()[contains(.,'balkon')]").get()
            if balcony:
                item_loader.add_value("balcony", True)

        room = response.xpath("//table[@class='BodyText']//tr[./td[.='omschrijving :']]/td[2]/text()[contains(.,'slaapkamer') or contains(.,'slaapkamers')]").get()
        if room:
            if "slaapkamers" in room:
                room = room.split("slaapkamers")[0].strip().split(" ")[-1]
            else:
                room = room.split("slaapkamer")[0].strip().split(" ")[-1]
            if room != "":
                item_loader.add_value("room_count", room)
        else:
            if "studio" in response.meta.get("property_type"):
                item_loader.add_value("room_count", "1")

        energy = response.xpath("//table[@class='BodyText']//tr[./td[contains(.,'EPC')]]/td[2]/text()[contains(.,'kWh')]").get()
        if energy:
            if energy.split(" ")[0].strip().isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy.split(" ")[0].strip()))
        
        utilities = response.xpath("//tr/td/b[contains(.,'kosten')]/parent::td/following-sibling::td/text()").get()
        if utilities:
            if "eur" in utilities.lower():
                utilities = utilities.lower().split("eur")[0].strip()
            elif "€" in utilities:
                utilities = utilities.split("€")[1].strip().split(" ")[0]            
            if utilities and utilities.isdigit():
                item_loader.add_value("utilities", utilities)
        
        item_loader.add_value("landlord_name", "De Smet & Poupeye")
        item_loader.add_value("landlord_phone", "050/367.000")
        item_loader.add_value("landlord_email", "francis.maertens@desmet-poupeye.be")
        
        status = response.xpath('//td[contains(.,"parkeerplaats")]//text()').get()
        if status:
            return
        else:
            yield item_loader.load_item()


# def split_address(address, get):
#     if "," in address:
#         temp = address.split(",")[1]
#         zip_code = "".join(filter(lambda i: i.isdigit(), temp))
#         city = temp.split(zip_code)[1].strip()

#         if get == "zip":
#             return zip_code
#         else:
#             return city

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