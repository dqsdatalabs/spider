# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import *
import dateparser
import requests
from scrapy import Request


class VastgoedwalthervanhoofSpider(scrapy.Spider):
    name = "vastgoedwalthervanhoof"
    allowed_domains = ["welkomthuis.be"]
    start_urls = ("https://www.welkomthuis.be/te-huur",)
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","

    # def sub_string_between(self, source, s1, s2):
    #     tmp = source[source.index(s1) + len(s1) :]
    #     return tmp[: tmp.index(s2)]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for link in response.xpath("//div[@class='s-grid smC15567SmSn1p3T3-style s-whise__grid']/a"):
            url = response.urljoin(link.xpath("./@href").extract_first())
            property_type = "".join(link.xpath("./div//div[@class='s-text smC15567SmSn1p3T8-style']/text()").getall())
            room = "".join(link.xpath("./div//div[@class='s-text smC15567SmSn1p3T12-style']/i[@class='fa fa-bed']/following-sibling::text()").getall())
            bathroom = "".join(link.xpath("./div//div[@class='s-text smC15567SmSn1p3T12-style']/i[@class='fa fa-shower']/following-sibling::text()").getall())
            if "appartement" in property_type:
                property_type = "apartment"
            elif "huis" in property_type:
                property_type = "house"
            else:
                property_type = ""
            if property_type:
                yield Request(
                    url,
                    
                    self.parse_detail,
                    dont_filter=True,
                    meta={
                        "room":room,
                        "bathroom":bathroom,
                        "property_type":property_type
                    }
                )

    def parse_detail(self, response):

        room = response.meta.get("room")
        bathroom = response.meta.get("bathroom")
        run_through = []
        run_through.extend(response.xpath(".//div[@class='container'][last()]/div[@class='row']"))
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("room_count", int(room))
        item_loader.add_value("bathroom_count", int(bathroom))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("address", "//title/text()")


        item_loader.add_xpath("description", ".//div[@class='container']/div[@class='row'][last()]//text()")
        item_loader.add_xpath("rent_string", "//div[@id='scomp265']//div[contains(@class,'smC15567SmSn1p3T38-style--Price')]/text()")
        item_loader.add_xpath("images", "//div[@class='s-slider']//img[@class='s-img__wrapper smC15567SmSn1p3T18-style glightbox']/@src")
        item_loader.add_xpath("floor_plan_images", ".//a[i[@class='fa fa-file-o']]/@href")
        city_zip = ", ".join(response.xpath("//div/h3[contains(.,'Geografische')]/following-sibling::text()[normalize-space()]").extract())
        if city_zip:
            city = city_zip.split(",")[-2].strip()
            zipcode = city.split(" ")[0]
            city = city.replace(zipcode,"")
            item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("city", city.strip())
        square = " ".join(response.xpath("//div[@class='row'][div[ contains(.,'Bebouwde oppervlakte') or contains(.,'opervlakte')]]/div[2]//text()").extract())
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        else:
            meters = " ".join(response.xpath("//dl//dt[contains(.,'Bewoonbare opp.')]/following-sibling::dd[1]/text()").extract())
            if meters:
                meters = int(float(meters))
                item_loader.add_value("square_meters",meters)
        utilities = " ".join(response.xpath("//div[@class='row'][div[ contains(.,'Lasten (€)')]]/div[2]//text()").extract())
        if utilities:
            item_loader.add_value("utilities", utilities)
        elif not utilities:
            utilities = response.xpath("//div[@class='container']/div[@class='row'][last()]//text()[contains(.,'kosten bedragen') and contains(.,'€')]").extract_first() 
            if utilities: 
                utilities = utilities.split(" bedragen")[1].strip()
                item_loader.add_value("utilities",utilities)
        dt = response.xpath(f".//div[@class='row'][div[contains(.,'Vrij vanaf')]]/div[2]//text()[normalize-space()]").get()
        if not dt:
            dt = response.xpath("substring-after(//div[@class='container']/div[@class='row'][last()]//text()[contains(.,'Beschikbaarheid:')],'Beschikbaarheid:')").get()
        if dt:
            dt = dateparser.parse(dt,date_formats=["%d/%m/%Y"])
            if dt:
                item_loader.add_value(
                    "available_date",
                    dt.date().strftime("%Y-%m-%d"),
                )
        parking = response.xpath("//dl//dt[contains(.,'Parking binnen')]/following-sibling::dd[1]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        terrace = response.xpath("//dl//dt[.='Terrassen']/following-sibling::dd[1]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
      
        item_loader.add_xpath("latitude", "//div[@class='s-google-maps']/@data-lat")
        item_loader.add_xpath("longitude", "//div[@class='s-google-maps']/@data-lng")
        item_loader.add_value("landlord_phone", "03 449 25 00")
        item_loader.add_value("landlord_name", "Vastgoed Walther Van Hoof")
        item_loader.add_value("landlord_email", "info@vastgoedvanhoof.be")
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        self.get_general(item_loader)
        self.get_from_detail_panel(
            " ".join(response.xpath(f".//div[@class='row'][div[not(contains(.,'nee'))]]/div[1]//text()").getall()),
            item_loader,
        )
        yield item_loader.load_item()

    def get_general(self, item_loader):
        keywords = {
            "address": "Adres",
            "external_id": "Referentie",
            # "square_meters": "Bebouwde oppervlakte",
            # "room_count": "Aantal slaapkamers",
            "bathroom_count": "Badkamers",
            "floor": "Verdiepingen",
            # "utilities": "Lasten (€)",
        }
        for k, v in keywords.items():
            item_loader.add_xpath(k, f".//div[@class='row'][div[ contains(.,'{v}')]]/div[2]//text()")

    def get_from_detail_panel(self, text, item_loader):
        """check all keywords for existing"""
        keywords = {
            "parking": [
                "parking",
                "garage",
                "parkeerplaatsen",
                "garagepoort",
                "parkeerplaats",
                "ondergrondse staanplaats",
            ],
            "balcony": ["balkon"],
            "pets_allowed": ["huisdieren toegelaten"],
            "furnished": ["gemeubileerd", "bemeubeld", "ingericht", "ingerichte", "gemeubeld"],
            "swimming_pool": ["zwembad"],
            "dishwasher": ["vaatwasser", "vaatwas", "afwasmachine"],
            "washing_machine": ["wasmachine"],
            "terrace": ["terras", "oriëntatie tuin/terras"],
            "elevator": ["lift", "elevator"],
        }

        value = remove_white_spaces(text).casefold()
        for k, v in keywords.items():
            if any(s in value for s in v):
                item_loader.add_value(k, True)

    # def sub_string_between(self, source, s1, s2):
    #     tmp = source[source.index(s1) + len(s1) :]
    #     return tmp[: tmp.index(s2)]