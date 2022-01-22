# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import *
import dateparser


class VbpartnersSpider(scrapy.Spider):
    name = "vbpartners"
    allowed_domains = ["vbpartners.be"]
    start_urls = (
        "https://www.vbpartners.be/residentieel/te-huur/appartement",
        "https://www.vbpartners.be/residentieel/te-huur/woning",
    )
    execution_type = "testing"
    country = "belgium"
    locale = "fr"
    thousand_separator = "."
    scale_separator = ","

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for item_responses in response.xpath(".//div[@id='estate-list']/div"):
            link = item_responses.xpath(".//div/@data-href").get()
            if link and "/verkopen-verhuren" not in link:
                yield scrapy.Request(
                    response.urljoin(link),
                    self.parse_detail,
                    cb_kwargs=dict(property_type="house" if "woning" in response.url else "apartment"),
                )

    def parse_map(self, response, item_loader):
        """ parse geo info"""
        geo = re.search(r"\d+\.\d{5,},\s*\d+\.\d{5,}", response.text)
        if geo:
            geo = geo.group().split(",")
            item_loader.add_value("latitude", geo[0])
            item_loader.add_value("longitude", geo[1])
            # self.get_from_geo(item)

    def parse_detail(self, response, property_type):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        item_loader.add_xpath("title", ".//div[@class]/h1//text()")
        item_loader.add_xpath("images", ".//div[@class='detail-slider']/a/@href")
        item_loader.add_xpath("rent_string", ".//div[@class='right']//text()")
        item_loader.add_xpath("description", ".//div[@class][h2]/text()")
        self.parse_map(response, item_loader)

        address = response.xpath("//div[contains(text(),'Adres:')]/following-sibling::div/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.strip().split(',')[-1].strip().split(' ')[0]
            city = address.strip().split(zipcode)[1].strip().split(" ")[0]
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        floor = response.xpath("//div[contains(text(),'Verdieping:')]/following-sibling::div/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        elevator = response.xpath("//div[contains(text(),'Lift:')]/following-sibling::div/text()").get()
        if elevator:
            if elevator.strip().lower() == 'ja':
                item_loader.add_value("elevator", True)
            elif elevator.strip().lower() == 'nee':
                item_loader.add_value("elevator", False)


        dt = response.xpath('.//div[div[@class="left"][contains(.,"Beschikbaarheid")]]/div[2]//text()').get()
        if dt:
            dt = dateparser.parse(dt, ["%d-%m-%Y"])
            if dt:
                item_loader.add_value(
                    "available_date",
                    dt.date().strftime("%Y-%m-%d"),
                )
        self.get_from_detail_panel(
            " ".join(response.xpath(f'.//div[div[@class="right"][not(contains(.,"Nee"))]]/div[1]//text()').getall()),
            item_loader,
        )
        self.get_from_detail_panel(
            " ".join(response.xpath(f'.//div[div[@class="right"][contains(.,"Nee")]]/div[1]//text()').getall()),
            item_loader,
            bool_value=False,
        )

        self.get_general(item_loader)
        item_loader.add_value("landlord_name", "VB Partners Immobiliën")
        item_loader.add_value("landlord_phone", "+32 (0) 3 337 50 70")
        item_loader.add_value("landlord_email", "antwerpen@vbpartners.be")

        yield item_loader.load_item()

    def get_general(self, item_loader):
        keywords = {
            "external_id": "Referentie",
            # "floor": "Verdieping",
            "utilities": "Maandelijkse lasten",
            # "address": "Adres:",
            # "available_date": "Disponibilitï",
            "room_count": "Aantal slaapkamers",
            "bathroom_count": "Aantal badkamers",
            "square_meters": "Bewoonbare opp",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(k, f'.//div[div[@class="left"][contains(.,"{v}")]]/div[2]//text()')

    def get_from_detail_panel(self, text, item_loader, bool_value=True):
        if not hasattr(self, "key_set"):
            self.key_set = set()
        """check all keywords for existing"""
        keywords = {
            "parking": [
                "parking",
                "garage",
                "car",
                "aantal garage",
            ],
            "balcony": [
                "balkon",
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
            # "elevator": ["ascenseur", "elevator"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                if bool_value == False and k not in self.key_set:
                    item_loader.add_value(k, bool_value)
                else:
                    item_loader.add_value(k, bool_value)
                self.key_set.add(k)