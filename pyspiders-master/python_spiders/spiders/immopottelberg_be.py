# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re


class MySpider(Spider):
    name = "immopottelberg_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source = 'Immopottelberg_PySpider_belgium_nl'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.immopottelberg.be/te-huur?categories=Residential", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                                callback=self.parse,
                                meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[contains(@title,'Meer')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, 
                            callback=self.populate_item, 
                            meta={'property_type': response.meta.get('property_type')})
            seen = True

        if page == 2 or seen:
            url = f"https://www.immopottelberg.be/te-huur?pageindex={page}&categories=Residential"
            yield Request(url, 
                            callback=self.parse, 
                            meta={"page": page + 1, 
                                    'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title/text()")

        prop = response.xpath("//tr[td[.='Type:']]/td[2]/text()").extract_first()
        if "Studio" in prop:
            item_loader.add_value("property_type", "Studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))

        square = response.xpath(
            "//tr[td[.='Bewoonbare opp.:']]/td[@class='kenmerk']/text()"
        ).get()

        desc = " ".join(
            response.xpath("//div[@class='col-xs-12']/p/text()").extract()
        ).strip()
        item_loader.add_value("description", desc)

        rent = response.xpath("//td[contains(.,'Huurprijs:')]/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("/")[0].replace("€","").replace(".","").split("/")[0])
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath(
            "external_id", "//tr[td[.='Referentie:']]/td[@class='kenmerk']/text()"
        )
        
        address = response.xpath("normalize-space((//h1/text())[1])").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip().split(' ')[0])
            item_loader.add_value("city", address.split(',')[-1].strip().split(' ')[-1])

        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])
    
        item_loader.add_xpath(
            "available_date", "//tr[td[.='Beschikbaar vanaf:']]/td[@class='kenmerk']/text()[not (contains(.,'In overleg'))][.!='Onmiddellijk']"
        )
        item_loader.add_xpath("floor", "//tr[td[.='Bouwlagen:']]/td[@class='kenmerk']")
        
        utilities = "".join(response.xpath("substring-before(substring-after(//tr[td[.='Totale kosten:']]/td[2]/text(),'€'),'/')").extract())
        if utilities:
            item_loader.add_value("utilities", utilities)

        item_loader.add_xpath("bathroom_count", "//tr[td[.='Badkamers:']]/td[2]/text()[.!='Ja']")
        item_loader.add_xpath("energy_label", "//tr[td[.='Energielabel:']]/td[2]/text()")

        room = response.xpath("//tr[td[.='Slaapkamers:']]/td[@class='kenmerk']/text()").get()
        if room:
            if "studio" in item_loader.get_collected_values("property_type") and "0" in room:
                item_loader.add_value("room_count", "1")
            else:
                item_loader.add_value("room_count", room)
        elif 'slaapkamer' in desc:
            room_count = re.search(r'(\d)\sslaapkamer', desc)
            if room_count:
                item_loader.add_value("room_count", room_count.group(1))
        else:
            room_count = response.xpath("//h3[contains(.,'slaapkamer')]/text()").re_first(r"\d")
            if room_count:
                item_loader.add_value("room_count", room_count)
        images = [
            response.urljoin(x)
            for x in response.xpath(  
                "//div[@class='owl-carousel']/a/@href"
            ).extract()
        ]
        item_loader.add_value("images", images)

        terrace = response.xpath(
            "//tr[td[.='Lift:']]/td[@class='kenmerk']/text()"
        ).get()

        if terrace:
            if terrace == "Ja":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)

        furnished = response.xpath("//tr[td[.='Gemeubeld:']]/td[2]/text()").get()
        if furnished:
            if furnished == "Ja":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        
        if "terrassen" in desc:
            item_loader.add_value("terrace", True)


        parking = response.xpath("//tr[td[.='Gemeubeld:']]/td[2]/text()").get()
        if parking:
                item_loader.add_value("parking", True)


        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'team')]//a[1]/text()")
        item_loader.add_xpath("landlord_email", "//div[contains(@class,'team')]//a[2]/text()")
        item_loader.add_xpath("landlord_name", "//div[contains(@class,'team')]/h3/text()")

        yield item_loader.load_item()


def split_address(address, get):

    if " " in address:
        temp = address.split(" ")[-2]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp))
        city = address.split(" ")[-1]

        if get == "zip":
            return zip_code
        else:
            return city
