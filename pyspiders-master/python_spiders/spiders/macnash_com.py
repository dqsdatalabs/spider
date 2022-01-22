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
import re

class MySpider(Spider):
    name = "macnash_com"
    start_urls = ["https://www.macnash.com/FR/a-louer/terrain.aspx"]  # LEVEL 1
    custom_settings = {
        "PROXY_ON": True,
        "PASSWORD": "wmkpu9fkfzyo",
    }
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    external_source = 'Macnash_PySpider_belgium_fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.macnash.com/ui/propertyitems.aspx?Page=0&Sort=0&ZPTID=1&TT=1", "property_type": "apartment", "type":"1"},
            {"url": "https://www.macnash.com/ui/propertyitems.aspx?Page=0&Sort=0&ZPTID=3&TT=1", "property_type": "house", "type":"3"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            'type':url.get("type")})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 1)
        
        seen = False
        for item in response.xpath("//div[@id='container']/div"):
            address = item.xpath(".//h3/a/text()").extract_first()
            follow_url = response.urljoin(
                item.xpath(".//div[@class='image']/a/@href").extract_first()
            )
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get("property_type"), "address": address},
            )
            seen = True

        if page == 1 or seen:
            url = f"https://www.macnash.com/ui/propertyitems.aspx?Page={page}&Sort=0&ZPTID={response.meta.get('type')}&TT=1"
            yield Request(url, 
                            callback=self.parse, 
                            meta={"page": page + 1, 
                                'type':response.meta.get("type"),
                                "property_type": response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        address = response.meta.get("address")
        item_loader.add_value("external_source", "Macnash_PySpider_" + self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        room = response.xpath("//div[@class='property--intro']/div[@class='field-name-era-aantal-slaapkamers--c field-type-text']//div[@class='era-tooltip-field']/text()[ .!='0'] |//tr[td[.='Number of bedrooms']]/td[2]/text()[ .!='0']").extract_first()
        square = response.xpath("//tr[@id='contentHolder_surfaceZone']/td[2]/text()").get()
        
        description = "".join(response.xpath("//div[@class='content']/p/text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description.strip())
        else:
            description = response.xpath("//meta[@name='description']/@content").get()
            if description:
                description = re.sub('\s{2,}', ' ', description.strip())
                item_loader.add_value("description", description)
        
        price = response.xpath(
            "//h3[@class='lead']//text()[contains(., '€')]"
        ).extract_first()
        if price:
            item_loader.add_value("rent", price.split("€")[0].replace(",", ""))
        item_loader.add_value("currency", "EUR")

        ref = response.xpath("//p[@class='reference']/text()").get()
        ref = ref.split(" ")[1]
        item_loader.add_value("external_id", ref)

        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])
        
        item_loader.add_xpath("bathroom_count", "//tr[@id='contentHolder_bathroomsZone']/td[2]/text()")
        
        if room != "0":
            item_loader.add_value("room_count",room)
        else:
            room_desc = response.xpath("//div[@class='content']/p/text()[contains(.,'studio')]").get()
            if room_desc:
                item_loader.add_value("room_count","1")
        
        item_loader.add_xpath(
            "available_date",
            "normalize-space(//table[@class='BodyText']//tr[./td[.='vrij :']]/td[2]/text())",
        )

        item_loader.add_value("address", address)
        item_loader.add_value("zipcode", split_address(address, "zip"))
        item_loader.add_value("city", split_address(address, "city"))
        utilities = response.xpath("//tr[./td[.='Charges (€)']]/td[2]/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0])
        item_loader.add_xpath(
            "floor", "//tr[@id='contentHolder_floorZone']/td[2]/text()"
        )

        item_loader.add_xpath(
            "longitude", "//tr[td[.='Xy coordinates']][1]/td[2]/text()"
        )
        item_loader.add_xpath(
            "latitude", "//tr[td[.='Xy coordinates']][2]/td[2]/text()"
        )

        energy = response.xpath("//tr[td[.='Energy certificate']][last()]/td[2]/text()").extract_first()
        if energy:
            energy = energy.replace("+","").replace("-","")
            if energy.isalpha():
                item_loader.add_value("energy_label", energy.upper())
               
        images = [
            response.urljoin(x)
            for x in response.xpath("//div[@id='lightgallery']/a/img/@src").extract()
        ]
        if images:
            item_loader.add_value("images", images)

        terrace = response.xpath(
            "//tr[td[.='Terrace']]/td[.='Yes']/text() | //tr[@id='contentHolder_interiorList_detailZone_3']/td[2]/text()"
        ).get()
        if terrace:
            if terrace == "Yes":
                item_loader.add_value("terrace", True)
            elif terrace == "No":
                item_loader.add_value("terrace", False)

        parking = response.xpath("//td[contains(.,'Parking') or contains(.,'Garage')]//following-sibling::td//text()").get()
        if parking and int(float(parking)) > 0: item_loader.add_value("parking", True)
        else:
            parking = " ".join(response.xpath("//tr[@id='contentHolder_parkingZone']/td[2]/text() | //tr[td[contains(.,'parking')]]/td[2]/text()").getall())
            if parking:
                if "yes" in parking.lower():
                    item_loader.add_value("parking", True)
                elif parking.isdigit() and parking > "1":
                    item_loader.add_value("parking", True)

        elevator = response.xpath(
            "//tr[td[.='Elevator']]/td[2]/text()"
        ).get()
        if elevator:
            if "Yes" in elevator.strip():
                item_loader.add_value("elevator", True)
            elif "No" in elevator.strip():
                item_loader.add_value("elevator", False)
        
        furnished = response.xpath(
            "//tr[@id='contentHolder_furnishedZone']/td[2]/text()"
        ).get()
        if furnished:
            if furnished == "Yes":
                item_loader.add_value("furnished", True)
            elif furnished == "No":
                item_loader.add_value("furnished", False)

        dishwasher = response.xpath(
            "//div[@class='content']/p/text()[contains(.,'lave-vaisselle')]"
        ).get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath(
            "//div[@class='content']/p/text()[contains(.,'machine à laver')]"
        ).get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        balcony = response.xpath(
            "//div[@class='content']/p/text()[contains(.,'balcon')]"
        ).get()
        if balcony:
            item_loader.add_value("balcony", True)

        phone = response.xpath("//p[@class='print']/text()[contains(.,'+')]").get()
        if phone:
            if ":" in phone:
                phone = phone.split(":")[1].strip()
            item_loader.add_value("landlord_phone", phone)
        email = response.xpath("//p[@class='print']/text()[contains(.,'@')]").get()
        if email:
            item_loader.add_value("landlord_email", email)
        item_loader.add_value("landlord_name", "Macnash Associates")

        swimming = response.xpath(
            "//div[@class='content']/p/text()[contains(.,'piscine')]"
        ).get()
        if swimming:
            item_loader.add_value("swimming_pool", True)
        yield item_loader.load_item()


def split_address(address, get):
    # temp = address.split(" ")[0]
    zip_code = "".join(filter(lambda i: i.isdigit(), address))
    city = address.split(zip_code)[1].strip()

    if get == "zip":
        return zip_code
    else:
        return city
