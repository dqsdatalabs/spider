# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from scrapy import Request
import re
import dateparser
import math

class MySpider(Spider):
    name = "rzk_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.rzk.be/nl/verhuur/woning/", "property_type": "house"},
            {"url": "https://www.rzk.be/nl/verhuur/appartement/", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):

        for item in response.xpath(
            "//div[@id='tab-list']/ul/li//div[@class='photo first']/a/@href"
        ).extract():
            follow_url = response.urljoin(item)
           
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get("property_type")
        prop_type = response.xpath("//div[contains(@class,'page-title')]/h3/text()").extract_first()
        if "Studio" in prop_type:
            property_type = "studio"
        item_loader.add_value("property_type", property_type)    
        item_loader.add_value("external_source", "Rzk_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)
        title = "".join(
            response.xpath("//div[contains(@class,'text-center')]/h1/text()").extract()
        )
        title = re.sub('\s{2,}', ' ', title)
        item_loader.add_value(
            "title", title.strip()
        )

        desc = " ".join(
            response.xpath(
                "//div[@class='detail-info-block']/div/span/text()"
            ).extract()
        )
        if desc:
            item_loader.add_value("description", desc.strip())
        if " wasmachine" in desc.lower():
            item_loader.add_value("washing_machine", True)
        if "vaatwasmachine" in desc.lower():
            item_loader.add_value("dishwasher", True)
        if "lift" in desc.lower():
            item_loader.add_value("elevator", True)
        if "balkon" in desc.lower():
            item_loader.add_value("balcony", True)
        
        
        price = response.xpath(
            "//tr[td[.='Prijs']]/td[@class='value'][contains(., '€')]"
        ).extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(".",""))

        utilities=response.xpath("//tr[@class='odd']/td[@class='value'][contains(., '€')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace("€","").strip())
        
        square = response.xpath(
            "//tr[td[.='Woonoppervlakte']]/td[@class='value']/text()"
        ).extract_first()
        if square :
            item_loader.add_value("square_meters", square.lstrip().rstrip().split("m²")[0])
        else:
            square = response.xpath("//div[@class='detail-info-block']/div/span/text()[contains(.,'m²')]").get()
            if square:
                square = square.strip().split("m²")[0].strip().split(" ")[-1]
                if "," in square:
                    square = square.replace(",", ".")
                    square = math.ceil(float(square))
                item_loader.add_value("square_meters", square)
        
        address = "".join(response.xpath("//div[contains(@class,'text-center')]/h1/text()").extract()).strip()
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city").strip())

        

        room = response.xpath(
            "//tr[td[.='Aantal slaapkamers']]/td[@class='value']/text()"
        ).get()
        if room:
            item_loader.add_value("room_count", room.lstrip().rstrip())
        elif response.xpath("//h3[contains(.,'Studio')]/text()").get():
            item_loader.add_value("room_count", "1")
        
        bathroom=response.xpath("//tr[td[.='Aantal badkamers']]/td[@class='value']/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())
        
        available_date = response.xpath(
            "//tr[td[.='Vrij op']]/td[@class='value']/text()[. !='onmiddellijk' and .!= 'na opzeg huurovereenkomst']"
        ).get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d/%m/%Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        item_loader.add_xpath(
            "floor",
            "normalize-space(//tr[td[.='Verdieping']]/td[@class='value']/text())",
        )
        energy = response.xpath(
            "//table[@class='epc detail-fields']//td[@class='value']/text()[not(contains(.,'kWh'))][1]"
        ).get()
        if energy:
            item_loader.add_value("energy_label", energy.strip())

        terrace = response.xpath(
            "//tr[td[.='Terras']]/td/text()"
        ).extract_first()
        if terrace:
            if terrace is not None:
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//a[@rel='property-pictures']/@href"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)

        floor_images = [
            response.urljoin(x)
            for x in response.xpath(
                "//table[@class='downloads detail-fields']//tr[./td/a[.='Grondplan']]//a/@href"
            ).extract()
        ]
        if floor_images:
            item_loader.add_value("floor_plan_images", floor_images)

        parking = response.xpath(
            "//tr[td[.='Aantal parkeerplaatsen' or .='Aantal carports' or .='Aantal garages']]/td[@class='value'] | //tr[td[.='autostanplaats']]/td/text()"
        ).extract_first()
        if parking:
            item_loader.add_value("parking", True)
            
        elevator = response.xpath("//tr[td[.='Lift']]/td[@class='value']//text()").extract_first()
        if elevator:
            if "ja" in elevator:
                item_loader.add_value("elevator", True)
            
        furnished = response.xpath("//tr[td[.='Gemeubeld']]/td[@class='value']//text()").extract_first()
        if furnished:
            if "ja" in furnished:
                item_loader.add_value("furnished", True)
            
        phone = response.xpath(
            '//div[@id="footer-contact"]/div/a[contains(@href, "tel:")]/@href'
        ).get()
        if phone:
            item_loader.add_value("landlord_phone", phone.replace("tel:+", ""))
        item_loader.add_value("landlord_name", "Rzk")
        item_loader.add_value("landlord_email", "info@rzk.be")

        item_loader.add_xpath("latitude", "//div[@id='map']/@data-geolat")
        item_loader.add_xpath("longitude", "//div[@id='map']/@data-geolong")

        external_id=response.xpath("//span[@id='property-id']/@data-property-id").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        
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
