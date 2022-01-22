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
import dateparser
import math
import re

class MySpider(Spider):
    name = "immobastogne"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source = "Immobastogne_PySpider_belgium_nl"
    # headers = {
    #     'accept': '*/*',
    #     'accept-encoding': 'gzip, deflate, br',
    #     'accept-language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
    #     'sec-fetch-dest': 'empty',
    #     'sec-fetch-mode': 'no-cors',
    #     'cache-control': 'public, s-maxage=604800',
    #     'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36'
    #     }
    download_timeout = 120

    custom_settings = {
        "PROXY_ON": True,
        #"PROXY_PR_ON": True,
        "PASSWORD": "wmkpu9fkfzyo",
        "LOG_LEVEL":"INFO",
        "CONCURRENT_REQUESTS": 4,    
        "COOKIES_ENABLED": False,    
        "AUTOTHROTTLE_ENABLED": True,    
        "AUTOTHROTTLE_START_DELAY": .5,    
        "AUTOTHROTTLE_MAX_DELAY": 2,    
        "RETRY_TIMES": 3,           
        "DOWNLOAD_DELAY": 0,
    }


    headers = {
        'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding':'gzip, deflate, br',
        'accept-language':'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control':'max-age=0',
        'sec-fetch-dest':'document',
        'sec-fetch-mode':'navigate',
        'sec-fetch-site':'none',
        'sec-fetch-user':'?1',
        'upgrade-insecure-requests':'1',
        'user-agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1'
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.immobastogne.be/Rechercher/Appartement%20Locations%20/Locations/Type-03%7CAppartement/Localisation-/Prix-/Tri-PRIX%20ASC,COMM%20ASC,CODE", "property_type": "apartment"},
            {"url":"https://www.immobastogne.be/Rechercher/Maison%20Locations%20/Locations/Type-01%7CMaison/Localisation-/Prix-/Tri-PRIX%20ASC,COMM%20ASC,CODE", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'list-item')]"):
            
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            follow_url = follow_url.replace("/Lang-EN","")
            if "javascript" not in follow_url:
                property_type = item.xpath(".//div[@class='estate-type']/text()").get()
                property_type2 = ""
                if property_type is not None:
                    if "flat" in property_type.lower() or "studio" in property_type.lower():
                        property_type2 = "studio"
                yield Request(url=follow_url,
                            callback=self.populate_item,
                            meta={'property_type': response.meta.get('property_type'), "studio": property_type2})
        
        pagination = response.xpath("//li[contains(@class,'hidden')]/a[contains(@href,'PageNumber')]/@href").extract()
        if pagination:
            yield Request(pagination[-1],
                             callback=self.parse,
                             meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get("property_type")

        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        desc = "".join(response.xpath("//div[@class='col-md-6']/p//text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip().replace("\n","").replace("\t","").replace("\u00a0",""))
            item_loader.add_value("description", desc)

        if "de provision" in desc:
            utilities = desc.split("de provision")[0].strip().split(" ")[-1].replace("€","")
            item_loader.add_value("utilities", utilities)
        
        rent = response.xpath(
            "//tr[td[.='Price']]/td/text()[contains(., '€')]"
        ).extract_first()
        if rent:
            item_loader.add_value("rent", rent)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("currency", "EUR")
        item_loader.add_xpath("external_id", "//div[@class='ref-tag']//span/b//text()")

        square = response.xpath(
            "//tr[td[.='Net living area']]/td[2]/text()"
        ).extract_first()
        if square:
            square = square.split("m²")[0].strip()
            square = math.ceil(float(square))
            item_loader.add_value("square_meters", str(square))
        else:
            square = response.xpath("//tr[td[.='Gross living area']]/td[2]/text()").get()
            if square:
                square = square.split("m²")[0].strip()
                square = math.ceil(float(square))
                item_loader.add_value("square_meters", str(square))
            else:
                square = response.xpath("//tr[td[.='Plot surface']]/td[2]/text()").get()
                if square:
                    square = square.split("m²")[0].strip()
                    square = math.ceil(float(square))
                    item_loader.add_value("square_meters", str(square))
        
        room = response.xpath("//tr[td[.='Bedrooms']]/td[2]/text()").get()
        if room:
            if "+" in room:
                room = room.split("possible")[0].strip()
                room1 = room.split("+")[0].strip()
                room2 = room.split("+")[1].strip()
                room = int(room1) + int(room2)
            item_loader.add_xpath("room_count", str(room))
        
        date = response.xpath(
            "//tr[./td[.='Availability']]//td//text()[contains(.,'/')]"
        ).extract_first()
        if date:
            date_parsed = dateparser.parse(date, date_formats=["%d/%m/%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        energy_label = response.xpath(
            "//div[contains(@class,'container')]//div[1]/img/@src[contains(.,'PEB')]"
        ).extract_first()
        if energy_label:
            item_loader.add_value(
                "energy_label", energy_label.split("L/")[1].split(".")[0].upper()
            )

        # property_type = response.xpath("//h1/text()").get()
        if "studio" in property_type:
            item_loader.add_value("property_type", response.meta.get("studio"))
        else:
            item_loader.add_value("property_type", property_type)

        address = response.xpath(
            "//div[@id='adresse_fiche']/p/text()"
        ).extract_first()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", split_address(address, "zip"))
            item_loader.add_value("city", split_address(address, "city"))
        else:
            address = response.xpath("//h1/span/text()").get()
            if address:
                item_loader.add_value("address", address)

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='carousel-inner hvr-grow']/div/a/img/@src"
            ).extract()
        ]
        item_loader.add_value("images", images)

        terrace = response.xpath("//tr[td[.='Terrace']]/td[2]/text()").get()
        if terrace:
            if terrace == "Yes":
                item_loader.add_value("terrace", True)
            elif terrace == "No":
                item_loader.add_value("terrace", False)
            elif "m²" in terrace:
                item_loader.add_value("terrace", True)

        furnished = response.xpath("//tr[td[.='Furnished']]/td[2]/text()").get()
        if furnished:
            if furnished == "Yes":
                item_loader.add_value("furnished", True)
            elif furnished == "No":
                item_loader.add_value("furnished", False)
        else:
            furnished = response.xpath("//div[@class='col-md-6']/p//text()[contains(.,'Meublé') or contains(.,'meublé')]").get()
            if furnished:
                item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_name", "IMMO BASTOGNE")
        item_loader.add_value(
            "landlord_email",
            "info@immobastogne.be",
        )
        item_loader.add_value("landlord_phone", "061/21.70.91")

        item_loader.add_xpath("bathroom_count", "//tr[./td[.='Bathrooms']]//td[2]//text()")

        item_loader.add_xpath("floor", "//tr[td[contains(.,'floors')]]//td[2]//text()")
        
        pets = response.xpath("//div[@class='col-md-6']/p//text()[contains(.,'Animaux') or contains(.,'animaux')]").get()
        if pets:
            if "non" in pets:
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)
        
        parking = response.xpath("//tr[td[contains(.,'parking')]]/td[2]/text()").get()
        if parking:
            if "1" in parking:
                item_loader.add_value("parking", True)
            elif "0" in parking:
                item_loader.add_value("parking", False)
        else:
            parking = response.xpath("//div[@class='col-md-6']/p//text()[contains(.,'Garage') or contains(.,'garage') or contains(.,'parking')]").get()
            if parking:
                item_loader.add_value("parking", True)
            
        balcony = response.xpath("//div[@class='col-md-6']/p//text()[contains(.,'Balcon') or contains(.,'balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        swimming = response.xpath("//div[@class='col-md-6']/p//text()[contains(.,'Piscine') or contains(.,'piscine')]").get()
        if swimming:
            item_loader.add_value("swimming_pool", True)
        
        dishwasher = response.xpath("//div[@class='col-md-6']/p//text()[contains(.,'Lave-vaisselle') or contains(.,'lave-vaisselle')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing = response.xpath("//div[@class='col-md-6']/p//text()[contains(.,'Machine à laver') or contains(.,'machine à laver')]").get()
        if washing:
            item_loader.add_value("washing_machine", True)
        
        yield item_loader.load_item()


def split_address(address, get):
    if "-" in address:
        temp = address.split("-")[1]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp))
        city = temp.split(" ")[1]

        if get == "zip":
            return zip_code
        else:
            return city
