# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import dateparser
import re


class ImmotrebelSpider(scrapy.Spider):
    name = "immotrebel"
    allowed_domains = ["immotrebel.be"]
    start_urls = (
        "http://www.immotrebel.be/index.php?p=listerBiens&action=L&sector=M",
        "http://www.immotrebel.be/index.php?p=listerBiens&action=L&sector=A",
    )
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","

    def sub_string_between(self, source, s1, s2):
        tmp = source[source.index(s1) + len(s1) :]
        return tmp[: tmp.index(s2)]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse_list, 
                cb_kwargs=dict(property_type="house" if "sector=M" in url else "apartment"),
            )

    def parse_list(self, response, property_type):
        for link in response.xpath(".//div[@class='listing']//a/@href"):
            yield response.follow(
                link,
                self.parse_detail,
                cb_kwargs=dict(property_type=property_type),
            )

    def parse_detail(self, response, property_type):
        address = response.xpath(
            ".//div[contains(@class,'panel')][div[contains(@class,'panel-map')]]/div[@class='panel-heading']"
        )
        stats = response.xpath(
            ".//div[contains(@class,'panel')][div[@class='panel-heading']/b/text()[contains(.,'Description intérieure')]]/div[@class='panel-body']"
        )
        item_loader = ListingLoader(response=response)
        item_loader.add_xpath("external_id", "substring-after(//div[@class='panel-heading' and contains(.,'Réf')]/text(),':')")
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        item_loader.add_xpath("description", ".//div[@class='text-descr']/text()")
        item_loader.add_xpath("title", "./head/meta[@property='og:title']/@content")
        item_loader.add_xpath("rent_string", ".//div[@class='panel-body']/font/b/text()")

        item_loader.add_xpath("images", ".//div[@class='fotorama']/a/@href")
        item_loader.add_value(
            "latitude",
            re.findall(
                r"\d+.\d+",
                self.sub_string_between(response.xpath(".//div[@class='row']/script/text()").get(), "showMap(", ");"),
            )[0],
        )
        # if stats.xpath(".//text()[contains(.,'chambre')]"):
        #     item_loader.add_value(
        #         "room_count",
        #         self.format_integer(stats.xpath(".//text()[contains(.,'chambre')]").get().split("(s)")[0]),
        #     )
        room = response.xpath("//head/meta[@property='og:title']/@content").extract_first()
        if room:
            room_count = ""
            if "studio" in room.lower():
                room_count = "1"
            if "CHAMBRE" in room.upper():
                room_count = room.upper().split("CHAMBRE")[0].strip().split(" ")[-1].strip()
                if "UNE" in room_count:
                    room_count = "1"
                elif "DEUX" in room_count:
                    room_count = "2"
                elif "TROIS" in room_count:
                    room_count = "3"
                elif "QUATRE" in room_count:
                    room_count = "4"
            if room_count.isdigit():
                item_loader.add_value("room_count",room_count)
        
        roomcheck=item_loader.get_output_value("room_count") 
        if not roomcheck:
            room1=item_loader.get_output_value("description")
            room=room1.split("CHAMBRES")
            room=re.findall("\d+",room[0])
            item_loader.add_value("room_count",room[-1])



        if stats.xpath(".//text()[contains(.,'salle(s) de bains')]"):
            item_loader.add_value(
                "bathroom_count",
                self.format_integer(stats.xpath(".//text()[contains(.,'salle')]").get().split("(s)")[1]),
            )
        item_loader.add_value(
            "longitude",
            re.findall(
                r"\d+.\d+",
                self.sub_string_between(response.xpath(".//div[@class='row']/script/text()").get(), "showMap(", ");"),
            )[1],
        )
        item_loader.add_value("address", "".join(address.xpath(".//text()").getall()))
        city = response.xpath("substring-after(//div[contains(@class,'panel')][div[contains(@class,'panel-map')]]/div[@class='panel-heading']//text()[normalize-space()],' -')").get()
        if city:
            zipcode = city.strip().split(" ")[0]
            city = " ".join(city.strip().split(" ")[1:])
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())
        square = response.xpath("//div[@class='panel-body']//text()[normalize-space() and contains(.,'Surface habitable')]").get()
        if square:
            square = square.split(":")[1].strip().split(" ")[0]
            item_loader.add_value("square_meters", int(float(square.replace(",","."))))
        utilities = response.xpath("//div[@class='text-descr']//text()[contains(.,'PROVISION') and contains(.,'EURO') ]").get()
        if utilities:   
            item_loader.add_value("utilities", utilities)
        
        available_date = response.xpath("//div[@class='panel-body']/font//text()[contains(.,'Disponible')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(":")[1].replace("immédiatement","now"), languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            
        item_loader.add_value("landlord_phone", "071 81 72 72")
        item_loader.add_value("landlord_name", "Immo Trebel")
        item_loader.add_value("landlord_email", "info@immotrebel.be")
        self.get_from_detail_panel(
            " ".join(response.xpath(".//div[div[contains(.,'Description extérieure')]]//text()").getall()), item_loader
        )
        yield item_loader.load_item()

    def get_general(self, stats, item_loader):
        keywords = {
            "address": "Adresse",
            # "square_meters": "Superficie séjour",
            "floor": "Niveau",
            # "utilities": "Charges",
            # "room_count": "Chambres",
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
