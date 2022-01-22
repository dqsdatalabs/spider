# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser
import json
import re

class MySpider(Spider):
    name = "dakwoningbeleggingen_nl"
    start_urls = [
        "https://www.dakwoningbeleggingen.nl/aanbod/woningaanbod/huur/"
    ] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' 
    external_source='Dakwoningbeleggingen_PySpider_netherlands_nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.dakwoningbeleggingen.nl/aanbod/woningaanbod/beschikbaar/huur/pagina-1/appartement/",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.dakwoningbeleggingen.nl/aanbod/woningaanbod/beschikbaar/huur/pagina-1/woonhuis/",
                "property_type" : "house"
            },
        ]# LEVEL 1 
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for follow_url in response.css("a.aanbodEntryLink::attr(href)").extract():
            yield response.follow(follow_url, self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = ""
            if response.meta.get("property_type") == "apartment":
                url = f"https://www.dakwoningbeleggingen.nl/aanbod/woningaanbod/beschikbaar/huur/pagina-{page}/appartement/"
            elif response.meta.get("property_type") == "house":
                url = f"https://www.dakwoningbeleggingen.nl/aanbod/woningaanbod/beschikbaar/huur/pagina-{page}/woonhuis/"
            yield Request(
                url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type'), "page":page+1}
            )

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Dakwoningbeleggingen_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//h1[@class='street-address']//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        desc = "".join(response.xpath("//div[@id='Omschrijving']/text()").extract())
        item_loader.add_value("description", desc.strip())

        price = response.xpath("normalize-space(//span[@class='rentprice']/text())").get()
        if price:
            item_loader.add_value(
                "rent", price.split("€")[1].split(",")[0])
            item_loader.add_value("currency", "EUR")

        deposit = response.xpath("normalize-space(//span[@class='kenmerkName'][contains(.,'Waarborgsom')]/following-sibling::span/text())").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(",")[0])

        utilities = response.xpath("normalize-space(//span[@class='kenmerkName'][contains(.,'Servicekosten')]/following-sibling::span/text())").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(" ")[1].split(",")[0])

        available_date=response.xpath("substring-after(//span[span[.='Aanvaarding']]/span[2]/text(),'Per ')").get()

        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)    
        
        square = response.xpath(
            "normalize-space(//span[@class='kenmerk first woonoppervlakte']/span[2]/text())"
        ).get()
        if square:
            item_loader.add_value(
                "square_meters", square.split("m²")[0]
            )
        room_count = response.xpath(
            "normalize-space(//span[@class='kenmerk  aantalslaapkamers']/span[2]/text())"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        street = response.xpath("//h1[@class='street-address']/text()").get()
        city = response.xpath("//span[@class='locality']/text()").get()
        zipcode = response.xpath("//span[@class='postal-code']/text()").get()
        if street or city:
            item_loader.add_value("address", street + " " + zipcode + " " + city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", city)
            
        available_date = response.xpath(
            "//span[@class='kenmerk  aanvaarding']/span[2]/text()[not(contains(.,'Direct')) and not(contains(.,'In overleg')) and not(contains(.,'in overleg')) and not(contains(.,'direct'))]").get()
        if available_date:
            available_date = available_date.strip().split("Per")[1].strip()
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d-%m-%Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        floor = response.xpath("normalize-space(//span[@class='kenmerk  aantalwoonlagen']/span[2]/text())").get()
        if floor:
            floor= floor.split(" ")[0]
            item_loader.add_value("floor", floor)

        dishwasher = response.xpath(
            "//div[@id='Omschrijving']/text()[contains(.,'vaatwasser')]"
        ).get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        

        furnished = response.xpath(
            "normalize-space(//span[@class='kenmerk  huurspecificatie']/span[2]/text())"
        ).get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath(
            "normalize-space(//span[@class='kenmerk first parkeerfaciliteit']/span[2]/text())").get()
        if parking:
            item_loader.add_value("parking", True)

        
        item_loader.add_xpath("energy_label", "normalize-space(//span[@class='kenmerk first energieklasse']/span[2]/text())")

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@class='detailFotos']/span[@class='fotolist ']//a/@href"
            ).extract()
        ]
        if images:
            img = list(set(images))
            item_loader.add_value("images", img)

        floor_images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@id='Plattegronden']/span[@class='fotolist ']//a/@href"
            ).extract()
        ]
        if floor_images:
            item_loader.add_value("floor_plan_images", floor_images)
        
        
        item_loader.add_value("landlord_phone", "+31 30 207 21 30")
        item_loader.add_value("landlord_name", "DAK Woningbeleggingen")
        item_loader.add_value("landlord_email", "info@dakwoningbeleggingen.nl")

        lat_long= response.xpath("//script[contains(.,'latitude')]/text()").get()
        if lat_long:
            item_loader.add_value("latitude", lat_long.split('"GeoCoordinates",')[1].split(",")[0].split(":")[1])
            item_loader.add_value("longitude", lat_long.split('"GeoCoordinates",')[1].split(",")[1].split(":")[1].split("}")[0])
        status="".join(response.xpath("//div[contains(@class,'tehuur verhuurd')]//text()").getall())
        if "verhuurd" not in status.lower():
            yield item_loader.load_item()


