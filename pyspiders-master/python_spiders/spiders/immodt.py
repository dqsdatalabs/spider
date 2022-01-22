# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
 
import scrapy 
from ..loaders import ListingLoader
from ..helper import *
import dateparser 
from scrapy import Request,FormRequest
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from python_spiders.loaders import ListingLoader
import json

class ImmodtSpider(scrapy.Spider):
    name = "immodt"
    allowed_domains = ["immodt.be"]
    execution_type = "testing"
    country = "belgium"
    locale = "fr" 
    thousand_separator = "."
    scale_separator = ","
    external_source="Immodt_PySpider_belgium_fr"
 
    def start_requests(self):
        start_urls =[
        {"url":"https://www.immodt.be/Rechercher/Locations","property_type": "apartment"}
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'), callback=self.parse,meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs): 
        for link in response.xpath("//a[@class='zoom-cont2 hvr-grow']/@href").getall():
            yield Request(
                response.urljoin(link),
                self.parse_detail,
                headers=self.get_lang(),meta={'property_type': response.meta.get('property_type')}
                # cb_kwargs=dict(property_type="apartment"),
            )

    def parse_detail(self, response):
        stats = []
        for temp in response.xpath(".//div[@class='row']//div[@class='row']/div"):
            stats.append(temp)
        item_loader = ListingLoader(response=response) 
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        
        item_loader.add_xpath("description", ".//div[div/div[@id='carousel']]//p/text()")
        desc=response.xpath("//div[div/div[@id='carousel']]//p/text()").get()
        if desc and "commercial" in desc:
            return 
 
        item_loader.add_xpath("title", ".//h1[@class='liste-title']/text()")
        dt = response.xpath(
            ".//table[@class='table table-striped']//tr[td[contains(.,'Disponibilit')]]/td[2]//text()"
        ).get()

        if dt:
            dt = dateparser.parse(dt)
            if dt:
                item_loader.add_value(
                    "available_date",
                    dt.date().strftime("%Y-%m-%d"),
                )
                # item_loader.add_value("available_date",dt)
        item_loader.add_xpath(
            "rent_string", ".//table[@class='table table-striped']//tr[td//text()[contains(.,'Prix')]]/td[2]//text()"
        )

        address = response.xpath("normalize-space(//div[@id='adresse_fiche']/p/text())").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", "".join(address.split(' - ')[-1].strip().split(' ')[0]).strip())
            item_loader.add_value("city", address.split(',')[-1].strip().split(' ')[-1].strip())
        else:
            address1=response.xpath("//h1[@class='liste-title']/span/text()").get()
            if address1:
                item_loader.add_value("address",address1)
                item_loader.add_value("city",address1)

        deposit = response.xpath("//td[contains(.,'Garantie locative')]/following-sibling::td/text()").get()
        if deposit:
            rent = response.xpath("//td[contains(.,'Prix')]/following-sibling::td/text()").get()
            if rent and "€" in rent:
                rent = rent.split('€')[0].strip().replace(' ', '').replace(".","")
            else: 
                rent = response.xpath("//h1/text()[contains(.,'€')]").get()
                if rent: rent = rent.split("€")[0].strip().split(" ")[-1].replace(".","")
            deposit = int(deposit.strip().split(' ')[0].split("m")[0].strip()) * int(rent)
            item_loader.add_value("deposit", deposit)

        item_loader.add_xpath("external_id", ".//div[@class='ref-tag']//b//text()")
        item_loader.add_xpath("images", ".//div[@id='carousel']//div/a/@href")
        self.get_from_detail_panel(
            " ".join(
                response.xpath(
                    f".//table[@class='table table-striped']//tr[td[2][not(contains(.,'Non'))]]/td[1]/text()"
                ).getall()
            ),
            item_loader,
        )
        self.get_general(stats, item_loader)
        item_loader.add_value("landlord_phone", "04 / 232.07.00")
        item_loader.add_value("landlord_email", "info@immodt.be")
        item_loader.add_value("landlord_name", "IDT Immobilère")
        yield item_loader.load_item()

    def get_general(self, stats, item_loader):
        keywords = {
            "address": "Adresse",
            "square_meters": "Surface habitable nette",
            "floor": "Niveau",
            "utilities": "Charges",
            "room_count": "Chambres",
            "bathroom_count": "Salles de bain",
        }
        for k, v in keywords.items():
            for temp in stats:
                if temp.xpath(f".//tr[td[1]/text()[contains(.,'{v}')]]/td[2]/text()"):
                    item_loader.add_value(k, temp.xpath(f".//tr[td[1]/text()[contains(.,'{v}')]]/td[2]/text()").get())

    def get_from_detail_panel(self, text, item_loader):
        """check all keywords for existing"""
        keywords = {
            "parking": [
                "parking",
                "garage",
                "car",
                "aantal garage",
            ],
            "balcony": [
                "balcon",
                "nombre de balcon",
                "Nombre d",
                "balcony",
                "balcon arrière",
            ],
            "pets_allowed": ["animaux"],
            "furnished": ["meublé", "appartement meublé", "meublée"],
            "swimming_pool": ["piscine"],
            "dishwasher": ["lave-vaisselle"],
            "washing_machine": ["machine à laver", "lave linge"],
            "terrace": ["terrasse", "terrasse de repos", "terras"],
            "elevator": ["ascenseur", "ascenceur"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, True)

    def get_lang(self):
        return {
            "Accept-Language": self.locale,
        }
