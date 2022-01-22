# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import re
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import re


class ConceptpropertiesSpider(scrapy.Spider):
    name = "conceptproperties"
    allowed_domains = ["conceptproperties.be"]
    start_urls = (
        "http://www.conceptproperties.be/index.php?ctypmandatmeta=l&action=list&reference=&categories%5B%5D=Appartement&chambre_min=&prix_max=",
        "http://www.conceptproperties.be/index.php?ctypmandatmeta=l&action=list&reference=&categories%5B%5D=Maison&chambre_min=&prix_max=",
    )
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","
    external_source='Conceptproperties_PySpider_belgium'

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for link in response.css(".item-description"):
            yield scrapy.Request(
                response.urljoin(link.xpath("./a/@href").get()),
                self.parse_detail,
                headers=self.get_lang(),
                cb_kwargs=dict(property_type="apartment" if "Appartement" in response.url else "house"),
            )
        yield from self.parse_next(response)

    def parse_next(self, response):
        """parse next page """
        xpath = './/div[@id="filters"]//a[contains(.,"Suivant")]/@href'
        for link in response.xpath(xpath):
            yield response.follow(link, self.parse)

    def parse_detail(self, response, property_type):
        """parse detail page """
        main_block_xpath = ".//div[div[@id='desc']]"
        main_block = response.xpath(main_block_xpath)
        tmp = main_block.xpath(".//div[@id='desc']")
        detail_node = main_block.xpath(".//div[@id='details']")
        if len(main_block) == 1:
            item_loader = ListingLoader(response=response)
            item_loader.add_value(
                "external_source", self.external_source)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value(
                "external_id", response.xpath(".//div[@id='textbox']/p[2]//text()").get().split(":")[-1]
            ) 
            item_loader.add_value(
                "address", main_block.xpath("//*[@id='page-title']//h2/span//text()").get().split("-")[-1].strip()
            )
            item_loader.add_value(
                "city", main_block.xpath("//*[@id='page-title']//h2/span//text()").get().split("-")[-1].strip()
            )
            item_loader.add_value("rent", tmp.xpath(".//div[@id='textbox']/p[1]//text()").get())
            item_loader.add_value("currency", "EUR")

            item_loader.add_value("title", tmp.xpath(".//div[@class='headline']/h2//text()").get())
            item_loader.add_value("description", tmp.xpath("./p//text()").get())
            item_loader.add_xpath("images", ".//div[@id='sliderx']//ul[@class='slides']//img/@src")
            item_loader.add_xpath("utilities", f"{main_block_xpath}//li//text()[contains(.,'Charges')]")
            if main_block.xpath(".//li//text()[contains(.,'Chambres')]"):
                item_loader.add_value( 
                    "room_count",
                    "".join(re.findall(r"[0-9]+", main_block.xpath(".//li//text()[contains(.,'Chambres')]").get())),
                )
            else:
                room_count1=response.xpath("//li//text()[contains(.,'Chambre')]").get()
                if room_count1:
                    room_count1=re.findall("\d+",room_count1)
                    item_loader.add_value("room_count",room_count1)

            if main_block.xpath(".//li//text()[contains(.,'Salle de bains')]"):
                item_loader.add_value(
                    "bathroom_count",
                    "".join(
                        re.findall(r"[0-9]+", main_block.xpath(".//li//text()[contains(.,'Salle de bains')]").get())
                    ),
                )
            self.get_general(item_loader, main_block)
            self.get_from_detail_panel(" ".join(detail_node.xpath(".//text()").getall()), item_loader)
            self.get_from_detail_panel(" ".join(tmp.xpath(".//text()").getall()), item_loader)

            item_loader.add_value("landlord_name", "Concept Properties")
            item_loader.add_value("landlord_phone", "0472 / 17.99.57")
            yield item_loader.load_item()

    def get_general(self, item_loader, response):
        keywords = {
            "square_meters": "Surface habitable:",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(k, f".//li//text()[contains(.,'{v}')]")

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
            "dishwasher": ["lave vaisselle"],
            "washing_machine": ["machine à laver", "lave linge"],
            "terrace": ["terrasse", "terrasse de repos", "terras"],
            "elevator": ["lift", "elevator"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, True)

    def sub_string_between(self, source, s1, s2):
        tmp = source[source.index(s1) + len(s1) :]
        return tmp[: tmp.index(s2)]

    def get_lang(self):
        return {
            "Accept-Language": self.locale,
        }
