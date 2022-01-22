# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = "ad_immo_be" # LEVEL 1
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    external_source = 'Adimmo_PySpider_belgium_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.ad-immo.be/fr/a-louer?view=list&page=1&ptype=1", "property_type": "house"},
            {"url": "https://www.ad-immo.be/fr/a-louer?view=list&page=1&ptype=2", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath(
            "//div[contains(@class,'property-list')]/div[@class='row-fluid']/div[contains(@class,'span3 property')]/div[@class='pic']//a/@href"
        ).extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,
                    meta={'property_type': response.meta.get('property_type')})
            seen = True

        if page == 2 or seen:
            url = f"https://www.ad-immo.be/fr/a-louer?view=list&page={page}"
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get('property_type'), "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Adimmo_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("id=")[1].split("&")[0])
        
        
        title = response.xpath("//h3[@class='pull-left leftside']/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        commercial = "".join(response.xpath("//h3[@class='pull-left leftside']/text()").extract())
        if "commercial" in commercial:
            pass
        else:
            desc = " ".join(response.xpath("//div[@class='row-fluid']/div[@class='group']/div/div[@class='field']/text()").getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)
            
            if "EMPLACEMENT DE PARKING INTERIEUR" in desc:
                return
            
            rent = response.xpath(
                "normalize-space(//div[@class='span8']/h3[@class='pull-right rightside'])"
            ).get()

            if rent:
                # currency = rent.split(" ")[1]
                rent = rent.split(" ")[0]

                item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")

            address = response.xpath(
                "//div[@class='group']/div[@class='content']/div[div[.='Adresse']]/div[@class='value']/text()"
            ).extract_first()
            if address:
                item_loader.add_value("address", address)
                item_loader.add_value("zipcode", split_address(address, "zip"))
                item_loader.add_value("city", split_address(address, "city"))
            
            square = response.xpath(
                "//div[@class='group']/div[@class='content']/div[div[.='Superficie totale']]/div[@class='value']/text()"
            ).get()
            if square:
                square = square.split(" ")[0]
                item_loader.add_value("square_meters", square)

            room = response.xpath(
                "//div[@class='group']/div[@class='content']/div[div[.='Nombre de Chambre(s)']]/div[@class='value']/text()"
            ).get()
            if room:
                room = room.split(" ")[0]
                item_loader.add_value("room_count", room)
            
            bathroom_count = response.xpath(
                "//div/div[contains(.,'salle')]/parent::div/div[@class='value']/text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)
            
            # item_loader.add_xpath("bathroom_count", "//div[@class='group']/div[@class='content']/div[div[.='Nombre de salle(s) de bain']]/div[@class='value']/text()")

            images = [
                response.urljoin(x)
                for x in response.xpath(
                    "//div[@id='LargePhoto']/div//img/@src"
                ).extract()
            ]

            if images:
                item_loader.add_value("images", images)

            energy_label = response.xpath(
                "//div[@class='content']/div/img/@src"
            ).extract_first()
            if energy_label:
                label = energy_label.split("_")[1].split(".")[0]
                if label == "2":
                    item_loader.add_value("energy_label", "A+")
                if label == "3":
                    item_loader.add_value("energy_label", "A")
                if label == "4":
                    item_loader.add_value("energy_label", "B")
                if label == "5":
                    item_loader.add_value("energy_label", "C")
                if label == "6":
                    item_loader.add_value("energy_label", "D")
                if label == "7":
                    item_loader.add_value("energy_label", "E")
                if label == "8":
                    item_loader.add_value("energy_label", "F")
                if label == "9":
                    item_loader.add_value("energy_label", "G")

            utilities = response.xpath(
                "//div[@class='group span6']//div[@class='field' and ./div[contains(.,'Charges locataire')]]/div[@class='value']//text()"
            ).get()
            if utilities:
                utilities = utilities.split(" ")[0]
                item_loader.add_value("utilities", utilities)

            if not item_loader.get_collected_values("utilities"):
                utilities = response.xpath("//div[@class='name' and contains(.,'Commentaire')]/following-sibling::div[@class='value']/text()").get()
                if utilities:
                    utilities = utilities.split("€")[0].strip().split(" ")[-1]
                    if utilities.isdigit():
                        item_loader.add_value("utilities", utilities)
            
            parking = response.xpath(
                "//div[@class='group']//div[@class='field' and ./div[contains(.,'Garage')]]/div[@class='value']//text()"
            ).get()
            if parking:
                item_loader.add_value("parking", True)
            else:
                parking = response.xpath(
                "//div[@class='row-fluid']/div[@class='group']/div/div[@class='field']/text()[contains(.,'garage')]").get()
                if parking:
                    item_loader.add_value("parking", True)
            
            dishwasher = response.xpath(
                "//div[@class='row-fluid']/div[@class='group']/div/div[@class='field']/text()[contains(.,'lave-vaisselle')]").get()
            if dishwasher:
                item_loader.add_value("dishwasher", True)

            washing = response.xpath(
                "//div[@class='row-fluid']/div[@class='group']/div/div[@class='field']/text()[contains(.,'Machine à laver') or contains(.,'machine à laver')]").get()
            if washing:
                item_loader.add_value("washing_machine", True)

            furnished = response.xpath("//div[@class='row-fluid']/div[@class='group']/div/div[@class='field']/text()[contains(.,'meublé')]").get()
            if furnished:
                item_loader.add_value("furnished", True)
            
            swimming_pool = response.xpath(
            "//div[@class='row-fluid']/div[@class='group']/div/div[@class='field']/text()[contains(.,'piscine') or contains(.,'Piscine')]").get()
            if swimming_pool:
                item_loader.add_value("swimming_pool", True)

            terrace = response.xpath(
            "//div[@class='row-fluid']/div[@class='group']/div/div[@class='field']/text()[contains(.,'terrasse') or contains(.,'Terrasse')]").get()
            if terrace:
                item_loader.add_value("terrace", True)
            
            balcony = response.xpath(
            "//div[@class='row-fluid']/div[@class='group']/div/div[@class='field']/text()[contains(.,'balcon') or contains(.,'Balcon')]").get()
            if balcony:
                item_loader.add_value("balcony", True)


            elevator = response.xpath(
                "//div[@class='content']//div[@class='field' and ./div[.='Ascenseur']]/div[@class='value']//text()"
            ).get()
            if elevator:
                if elevator == "Oui":
                    item_loader.add_value("elevator", True)
                elif elevator == "Yes":
                    item_loader.add_value("elevator", True)
                elif elevator == "No":
                    item_loader.add_value("elevator", False)
                else:
                    item_loader.add_value("elevator", False)
            item_loader.add_value("landlord_name", "AD-immo")
            item_loader.add_value("landlord_email", "info@ad-immo.be")
            item_loader.add_value("landlord_phone", "+ 32 81 74 73 75")
            yield item_loader.load_item()


def split_address(address, get):
    if " " in address:
        temp = address.split(" ")[0]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp))
        city = address.split(" ")[1]

        if get == "zip":
            return zip_code
        else:
            return city
