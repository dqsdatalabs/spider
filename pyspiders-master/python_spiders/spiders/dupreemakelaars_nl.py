# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser
import re

class MySpider(Spider):
    name = "dupreemakelaars_nl"
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' 
    external_source = 'Dupreemakelaars_PySpider_netherlands_nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.dupreemakelaars.nl/aanbod/woningaanbod/huur/appartement/",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.dupreemakelaars.nl/aanbod/woningaanbod/huur/woonhuis/",
                "property_type" : "house"
            },
        ]# LEVEL 1
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        
        for follow_url in response.css("a.aanbodEntryLink::attr(href)").extract():
            yield response.follow(follow_url, self.populate_item, meta={'property_type': response.meta.get('property_type')})
        yield self.paginate(response)

    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        rented = response.xpath("//span[span[.='Status']]/span[@class='kenmerkValue']/text()[contains(.,'Verhuurd')]").get()
        if rented:
            return
        
        title = response.xpath("(//title/text())[1]").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        city = response.xpath("//span[@class='zipcity']/span[@class='locality']/text()").get()
        if city:
            item_loader.add_value("city", city)
        zipcode = response.xpath("//span[@class='zipcity']/span[@class='postal-code']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        address = response.xpath("//h1[@class='street-address']/text()").get()
        if address:
            address2 = address + " " + city + " " + zipcode
            if address2:
                item_loader.add_value("address", address2)
            else:
                item_loader.add_value("address", address)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        desc = "".join(response.xpath("//div[@id='Omschrijving']/text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.replace("\n"," "))
            if "terras " in desc:
                item_loader.add_value("terrace", True)
            if "gestoffeerd" in desc:
                item_loader.add_value("furnished", True)
            if "Geen huisdieren toegestaan" in desc:
                item_loader.add_value("pets_allowed", False)
            if "parkeerplaatsen" in desc:
                item_loader.add_value("parking", True)
            
        price = response.xpath("//span[@class='rentprice']/text()").get()
        if price:
            item_loader.add_value(
                "rent", price.split("€")[1].split(",")[0])
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//span[contains(@class,'waarborgsom')]/span[2]/text()").get()
        if deposit:
            deposit = re.findall(r'\d+(?:\.\d+)?', deposit)
            if deposit:
                item_loader.add_value("deposit", deposit[0])

        square = response.xpath(
            "normalize-space(//span[@class='kenmerk first woonoppervlakte']/span[2]/text())"
        ).get()
        if square:
            item_loader.add_value(
                "square_meters", square.split("m²")[0]
            )
        else:
            square = response.xpath("//span[contains(@class,'gebruiksoppervlakte')]/span[2]/text()[contains(.,'m²')]").get()
            if square:
                item_loader.add_value("square_meters", square.split("m²")[0])
        room_count = response.xpath(
            "normalize-space(//span[@class='kenmerk aantalslaapkamers' or @class='kenmerk  aantalkamers']/span[2]/text())"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        available_date = response.xpath(
            "normalize-space(//span[@class='kenmerk  oplevering']/span[2]/text())").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        floor = response.xpath("//span[contains(@class,'gelegenopwoonlaag')]/span[2]/text()").get()
        if floor:
            item_loader.add_value(
                "floor", floor.split(" ")[0]
            )

        utilities = response.xpath("//span[contains(@class,'servicekosten')]/span[2]/text()").get()
        if utilities:
            utilities = utilities.split("€")[1].replace(",",".").strip()
            item_loader.add_value("utilities", int(float(utilities)))

        
        parking = response.xpath(
            "//span[contains(@class,'parkeerfaciliteit')]/span[2]/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        elevator = response.xpath(
            "//span[@class='kenmerk first voorzieningwonen']/span[2]/text()[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath(
            "//div/span[contains(@class,'balkon')]/span[2]/text()").get()
        if balcony:
            if "Ja" in balcony:
                item_loader.add_value("balcony", True)

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//span[contains(@class,'fotolist')]/a//img/@src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        
        
        item_loader.add_value("landlord_phone", "0182-551255")
        item_loader.add_value("landlord_name", "Dupree Makelaars")
        item_loader.add_value("landlord_email", "gouda@dupree.nl")

        lat_long= response.xpath("//script[contains(.,'latitude')]/text()").extract()
        if lat_long:
            if len(lat_long) == 2:
                item_loader.add_value("latitude", lat_long[1].split('"GeoCoordinates",')[1].split(",")[0].split(":")[1]
                )
                item_loader.add_value("longitude", lat_long[1].split('"GeoCoordinates",')[1].split(",")[1].split(":")[1].split("}")[0])

        yield item_loader.load_item()

    # 3. PAGINATION LEVEL 1
    def paginate(self, response):
        next_page_url = response.css(
            "div.paginaNavigatie a::attr(href)"
        ).extract_first() 
        if next_page_url is not None:
            return response.follow(next_page_url, self.parse, meta={'property_type': response.meta.get('property_type')})
