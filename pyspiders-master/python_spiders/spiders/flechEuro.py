# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import *


class FlecheuroSpider(scrapy.Spider):
    name = "flechEuro"
    allowed_domains = ["flech-euro.be"]
    start_urls = (
        "https://www.flech-euro.be/fr/a-louer?view=list&page=1&ptype=1&goal=1",
        "https://www.flech-euro.be/fr/a-louer?view=list&page=1&ptype=2,3&goal=1",
    )
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for link in response.xpath(".//div[@class='pic']//a"):
            yield response.follow(
                link,
                self.parse_detail,
                cb_kwargs=dict(property_type="apartment" if "type=2,3" in response.url else "house"),
            )

    def parse_detail(self, response, property_type):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", self.sub_string_between(response.url, "id=", "&"))
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        item_loader.add_xpath(
            "rent_string", f".//div[div[@class='name'][contains(.,'Prix')]]/div[@class='value']/text()"
        )
        item_loader.add_xpath("title", ".//div[@class='row-fluid header']/div/h3[1]/text()")

        city = response.xpath("//div[@class='content']/div[div[.='Adresse']]/div[3]/text()").extract_first()
        if city:
            item_loader.add_value("zipcode", city.split(" ")[0].strip())
            item_loader.add_value("city", city.split(" ")[1].strip())

        item_loader.add_xpath("images", "//a[@class='colorBoxImg']/@href")
        item_loader.add_xpath("description", "//head/meta[@property='og:description']/@content")
        item_loader.add_value("landlord_phone", "+32 (0) 87 22 91 95")
        item_loader.add_value("landlord_name", "FLECH'EURO sprl")
        item_loader.add_value("landlord_email", "info@flech-euro.be")

        self.get_from_detail_panel(
            " ".join(
                response.xpath(
                    f".//div[div[@class='value'][not(contains(.,'Non'))]]/div[@class='name']/text()"
                ).getall()
            ),
            item_loader,
        )
        self.get_general(response, item_loader)
        yield item_loader.load_item()


    def sub_string_between(self, source, s1, s2):
        tmp = source[source.index(s1) + len(s1) :]
        return tmp[: tmp.index(s2)]

    def get_general(self, response, item_loader):
        keywords = {
            "address": "Adresse",
            "square_meters": "Superficie totale",
            "floor": "Niveau",
            "utilities": "Charges",
            "room_count": "Nombre de Chambre(s)",
            "bathroom_count": "Nombre de salle(s) de bain",
        }
        for k, v in keywords.items():
            if "count" in k:
                item_loader.add_value(
                    k,
                    response.xpath(f".//div[div[@class='name'][contains(.,'{v}')]]/div[@class='value']/text()").re(
                        "\d+"
                    ),
                )
            else:
                item_loader.add_xpath(k, f".//div[div[@class='name'][contains(.,'{v}')]]/div[@class='value']/text()")

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
