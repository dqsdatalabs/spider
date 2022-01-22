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
import dateparser
import re

class MySpider(Spider):
    name = "address_re_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source = 'Addressre_PySpider_belgium_nl'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.address-re.be/nl/te-huur?view=list&page=1&ptype=1&pricemin=0&minroom=0&maxroom=6&goal=1", "property_type": "house"},
            {"url": "https://www.address-re.be/nl/te-huur?view=list&page=1&ptype=3&pricemin=0&minroom=0&maxroom=6&goal=1", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})
    
    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='property-list']//div[contains(@class,'span2 property')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,
                    meta={'property_type': response.meta.get('property_type')})
            seen = True

        if page == 2 or seen:
            url = ""
            if 'ptype=1' in response.url:
                url = f"https://www.address-re.be/nl/te-huur?view=list&page={page}&ptype=1&pricemin=0&minroom=0&maxroom=6&goal=1"
            elif 'ptype=3' in response.url:
                url = f"https://www.address-re.be/nl/te-huur?view=list&page={page}&ptype=3&pricemin=0&minroom=0&maxroom=6&goal=1"
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get('property_type'), "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Addressre_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        title = response.xpath("//h3[@class='pull-left leftside']/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        desc="".join(response.xpath("//div[div[.='Omschrijving']]//div[@class='field']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.strip())
        if " wasmachine" in desc.replace("(","").lower():
            item_loader.add_value("washing_machine", True)
        if "afwasmachine" in desc.lower():
            item_loader.add_value("dishwasher", True)
        if "terras" in desc.lower():
            item_loader.add_value("terrace", True)
        if "gemeubileerd" in desc.lower():
            item_loader.add_value("furnished", True)
        if "lift" in desc.lower():
            item_loader.add_value("elevator", True)
        if "balkon" in desc.lower():
            item_loader.add_value("balcony", True)
        if 'verdieping' in desc.lower():
            item_loader.add_value("floor", "".join(filter(str.isdigit, desc.lower().split('verdieping')[0].strip().split(' ')[-1])))

        furnished = response.xpath("//div[contains(text(),'Gemeubileerd')]/following-sibling::div[@class='value']/text()").get()
        if furnished:
            if 'ja' in furnished.lower():
                item_loader.add_value("furnished", True)
            elif 'nee' in furnished.lower():
                item_loader.add_value("furnished", False)
        
        price = response.xpath(
            "//h3[@class='pull-right rightside']//text()[contains(., '€')]"
        ).extract_first()
        if price:
            item_loader.add_value("rent_string", price)

        property_type = response.xpath(
            "//h3[@class='pull-left leftside']/text()"
        ).extract_first()
        if property_type:
            property_type = property_type.split("(")[0].split(" ")[-4].strip()
            item_loader.add_value("property_type", property_type)
        item_loader.add_xpath(
            "external_id",
            "(//div[@class='value']//text())[1]",
        )
        square_meters = response.xpath(
            "//div[div[@class='group-container']]//div[@class='field' and ./div[@class='name' and .='Bewoonbare opp.']]/div[@class='value']/text()"
        ).extract_first()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0])

        room = response.xpath(
            "//div[div[@class='group-container']]//div[@class='field' and ./div[@class='name' and .='Aantal slaapkamers']]/div[@class='value']/text()"
        ).get()
        if room:
            item_loader.add_value("room_count", room)

        bathroom=response.xpath("//div[@class='field']/div[contains(.,'badkamers')]/following-sibling::div[@class='value']/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())

        available_date = response.xpath(
            "//div[div[@class='group-container']]//div[@class='field' and ./div[@class='name' and .='Beschikbaarheid']]/div[@class='value']/text()[. != 'Onmiddelijk' and . != 'Overeen te komen']"
        ).get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d/%m/%Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        utilities = response.xpath("//div[contains(text(),'Lasten huurder')]/following-sibling::div[@class='value']/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[-1].strip())

        terrace = response.xpath(
            "//div[div[.='Composition']]//div[@class='field']/div[@class='value']"
        ).get()
        if terrace:
            item_loader.add_value("terrace", True)

        terrace = response.xpath(
            "//div[div[@class='group-container']]//div[@class='field' and ./div[@class='name' and .='Gemeubileerd']]/div[@class='value' and .='Ja']"
        ).get()
        if terrace:
            if terrace == "Ja":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        terrace = response.xpath(
            "//div[div[@class='group-container']]//div[@class='field' and ./div[.='Garage']]/div[@class='value']"
        ).get()
        if terrace:
            item_loader.add_value("parking", True)

        elevator = response.xpath("//div[contains(text(),'Lift')]/following-sibling::div[@class='value']/text()").get()
        if elevator:
            if 'ja' in elevator.lower():
                item_loader.add_value("elevator", True)
            elif 'nee' in elevator.lower():
                item_loader.add_value("elevator", False)

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//ul[@class='thumbnails']/li/a/img/@src"
            ).extract()
        ]
        item_loader.add_value("images", images)

        address = response.xpath(
            "//div[div[@class='group-container']]//div[@class='field' and ./div[@class='name' and .='Adres']]/div[@class='value']/text()"
        ).extract_first()
        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", split_address(address, "zip"))
        item_loader.add_value("city", split_address(address, "city"))

        energy = response.xpath("//div[div[.='Stedenbouwkundige informatie']]//div[@class='field' and ./div[.='EPC']]/div[@class='value']/text()").get()
        if energy:
            e_label=energy_label_calculate(energy.split(" ")[0].split(".")[0])
            item_loader.add_value("energy_label",e_label)

        item_loader.add_value("landlord_name", "Address Real Estate")
        item_loader.add_value("landlord_email", "info@address-re.be")
        item_loader.add_value("landlord_phone", "+32 (0)2 64 62 561")

        yield item_loader.load_item()


def split_address(address, get):
    if "," in address:
        temp = address.split(",")[1]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp))
        city = temp.split(zip_code)[1]

        if get == "zip":
            return zip_code
        else:
            return city
    else:
        # temp = address.split(" ")[0]
        zip_code = "".join(filter(lambda i: i.isdigit(), address))
        city = address.split(zip_code)[1]

        if get == "zip":
            return zip_code
        else:
            return city

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