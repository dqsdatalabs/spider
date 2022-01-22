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
    name = "dejaegher"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl' 
    external_source = "Dejaegher_PySpider_belgium_nl"

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.dejaegher.com/nl/te-huur?view=list&page=1&ptype=2",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.dejaegher.com/nl/te-huur?view=list&page=1&ptype=1",
                "property_type" : "house"
            },
            
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})
    
    # 1. FOLLOWING
    def parse(self, response):

        # page = response.meta.get("page", 2)

        # seen = False
        for item in response.xpath("//div[@class='image']/a/@href").getall():
            follow_url = response.urljoin(item)
            # prop_type = follow_url.xpath("//h3/text()").get().split()[0]
            # if prop_type:
            #     if prop_type == "Appartement":
            #         prop_type = "apartment"         
            #     elif prop_type == "Huis":
            #         prop_type = "house"
            #     else:
            #         prop_type = ""

            #if prop_type and prop_type != "":
            yield Request(follow_url, callback=self.populate_item) 
         
        # if page == 2 or seen:
        #     url = f"https://www.dejaegher.com/nl/te-huur/andere?view=list&page={page}"
        #     yield Request(url, callback=self.parse, meta={"page": page + 1})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        prop_type = response.xpath("//h3/text()").get().split()[0]
        if prop_type:
            if prop_type == "Appartement":
                prop_type = "apartment"         
            elif prop_type == "Huis":
                prop_type = "house"
        item_loader.add_value('property_type', prop_type)
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//div[contains(@class,'header')]//h3[contains(@class,'pull-left')]//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        externalid=response.url
        if externalid:
            item_loader.add_value("external_id",externalid.split("&id=")[-1].split("&page")[0])
        desc = "".join(response.xpath("//div[@class='content description-e']/div/p//text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        if desc:
            if 'terras' in desc:
                item_loader.add_value("terrace", True)
            if 'parking' in desc.lower() or 'parkeerplaats' in desc.lower():
                item_loader.add_value('parking', True)

            email = re.search(r'e-mail\s(.*)\s', desc)
            if email:
                item_loader.add_value("landlord_email", email.group(1))
            phone = re.search(r'\d{3}/*\d{2}.\d{2}.\d{2}', desc)
            if phone:
                item_loader.add_value("landlord_phone", re.sub(r'\D', '', phone.group(0)))
            name = re.search(r'Bezoek:(.*Dejaegher)', desc)
            if name:
                item_loader.add_value("landlord_name", name.group(1).strip())
            else:
                item_loader.add_value("landlord_name", "Industrimmo Dejaegher")
        item_loader.add_value("external_link", response.url)
        parkingcheck=item_loader.get_output_value("parking")
        if not parkingcheck:
            parking=response.xpath("//div[.='Garage']/following-sibling::div/following-sibling::div/text()").get()
            if parking:
                item_loader.add_value("parking",True)
        rent = response.xpath(
            "//div[@class='accordion-inner']/div[div[.='Prijs']]/div[@class='value']/text()"
        ).get()
        if rent:
            item_loader.add_value("rent", rent.split(" ")[1])
        item_loader.add_value("currency", "EUR")

        address = response.xpath(
            "//div[@class='accordion-inner']/div[div[.='Adres']]/div[@class='value']/text()"
        ).extract_first()
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", split_address(address, "zip"))
        item_loader.add_value("city", split_address(address, "city"))

        square = response.xpath(
            "//div[@class='accordion-inner']/div[./div[.='Grondoppervlakte' or .='Bewoonbare opp.']]//div[@class='value']/text()"
        ).get()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])

        room_count = response.xpath("//div[contains(., 'slaapkamers')]/div[@class='value']/text()").get()
        if room_count:
            item_loader.add_value('room_count', room_count)
        bathroom = response.xpath("//div[contains(., 'badkamers')]/div[@class='value']/text()").get()
        if bathroom:
            item_loader.add_value('bathroom_count', bathroom)
        # prop = response.xpath(
        #     "normalize-space(//h3[@class='pull-left leftside']/text())"
        # ).get()
        # if prop:
        #     item_loader.add_value("property_type", prop.split("te")[0])

        item_loader.add_xpath(
            "available_date",
            "//div[@class='accordion-inner']/div[div[.='Beschikbaarheid']]/div[@class='value']",
        )

        images = [response.urljoin(x) for x in response.xpath("//img/@src").extract()]
        img = list(set(images))
        item_loader.add_value("images", img)

        deposit = response.xpath("//p[contains(., 'Waarborg')]//span[2]/text()").get()
        if deposit and rent:
            rent_int = int(re.sub(r'\D','',rent))
            deposit = int(re.sub(r'\D', '', deposit)) * rent_int            
            item_loader.add_value("deposit", deposit)
       
        yield item_loader.load_item()


def split_address(address, get):
    if address is not None:
        if "," in address:
            temp = address.split(",")[1]
            zip_code = "".join(filter(lambda i: i.isdigit(), temp))
            city = address.split(",")[0]

            if get == "zip":
                return zip_code
            else:
                return city
        if " " in address:
            temp = address.split(" ")[0]
            zip_code = "".join(filter(lambda i: i.isdigit(), temp))
            city = address.split(" ")[1]

            if get == "zip":
                return zip_code
            else:
                return city
