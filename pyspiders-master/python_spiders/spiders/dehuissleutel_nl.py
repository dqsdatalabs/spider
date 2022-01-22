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
    name = "dehuissleutel_nl"
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'  # LEVEL 1
    external_source='Dehuissleutel_PySpider_netherlands_nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.dehuissleutel.nl/nl/zoeken/panden?city=false&object=2&min=false&max=false&x=40&y=9",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.dehuissleutel.nl/nl/zoeken/panden?city=false&object=1&min=false&max=false&x=49&y=12",
                "property_type" : "house"
            },
            {
                "url" : "https://www.dehuissleutel.nl/nl/zoeken/panden?city=false&object=4&min=false&max=false&x=50&y=7",
                "property_type" : "student_apartment"
            },
            {
                "url" : "https://www.dehuissleutel.nl/nl/zoeken/panden?city=false&object=7&min=false&max=false&x=35&y=11",
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[@id='building-overview']/div[contains(@class,'overview_item')]"):
            follow_url = response.urljoin(item.xpath("./a/@href").extract_first())
            square = item.xpath("substring-after(.//p[contains(.,'Woonoppervlakte')]/text(), ': ')").extract_first()
            date = item.xpath("substring-after(.//p[contains(.,'Aanvaarding')]/text(), ': ')").extract_first()
            room = item.xpath("substring-after(.//p[contains(.,'Aantal slaapkamers ')]/text(), ': ')").extract_first()
            furnished = item.xpath("substring-after(.//p[contains(.,'Interieur ')]/text(), ': ')").extract_first()
            deposit = item.xpath("substring-after(.//p[contains(.,'Borg')]/text(), ': ')").extract_first()
            
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta.get("property_type"), "square": square, "date": date, "room": room, "furnished": furnished, "deposit": deposit})
            seen = True
        
        if page == 1 or seen:
            url = response.url.split("&page")[0] + f"&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type":response.meta.get("property_type")})


    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Dehuissleutel_PySpider_" + self.country + "_" + self.locale)
        
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("property_type", response.meta.get("property_type"))

        item_loader.add_value("external_link", response.url)
        deposit = response.meta.get("deposit")
        if deposit:
            deposit = deposit.strip("€").strip()
            item_loader.add_value("deposit", deposit)
        utilities = response.xpath("//div[@id='tab0']/div[@class='dynamic-input']/p/text()[contains(.,'gas water en licht bedraagt') and contains(.,'EUR') and not(contains(.,'Wassen'))]").get()
        if utilities:
            if "totale" not in utilities:           
                utilities_numbers = re.findall(r'\d+(?:\.\d+)?', utilities.split("EUR")[1].replace(",","."))
                if utilities_numbers:                 
                    item_loader.add_value("utilities", int(float(utilities_numbers[0])))

        desc = "".join(response.xpath("//div[@id='tab0']/div[@class='dynamic-input']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "Géén huisdieren" in desc or "Geen huisdieren toegestaan" in desc:
                item_loader.add_value("pets_allowed", False)
            if "dakterras" in desc.lower():
                item_loader.add_value("terrace", True)
            if "badkamer" in desc.lower():
                item_loader.add_value("bathroom_count", "1")
        else:
            pets =  "".join(response.xpath("//div[@id='tab0']/div[@class='dynamic-input']//text()[contains(.,'Huisdieren toegestaan') or contains(.,'huisdieren toegestaan') ]").extract())
            if pets:
                item_loader.add_value("pets_allowed", True)

        price = response.xpath("normalize-space(//p[@class='typeface-js price']/text())").get()
        if price:
            price = price.split("€")[1].split(",")[0]
            if price != " ":
                item_loader.add_value(
                    "rent", price)
        item_loader.add_value("currency", "EUR")

        
        item_loader.add_value(
            "square_meters", response.meta.get("square").split("m2")[0]
        )
        
        if response.meta.get("room") != "0":
            item_loader.add_value("room_count", response.meta.get("room"))
        elif response.meta.get("property_type") == "studio":
            item_loader.add_value("room_count", "1")
        elif response.meta.get("property_type") == "student_apartment":
            item_loader.add_value("room_count", "1")

        street = response.xpath("//h1[@class='detail']/text()").get()
        city = response.xpath("//h2[@class='detail']/text()").get()
        item_loader.add_value("address", street + " " + city)
        item_loader.add_value("zipcode", split_address(city, "zip"))
        item_loader.add_value("city", split_address(city, "city"))
            
        available_date = response.meta.get("date")
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%m-%d-%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        furnished = response.meta.get("furnished")
        if furnished:
            if "Gestoffeerd" in furnished:
                item_loader.add_value("furnished", True)
        
        parking = response.xpath(
            "//div[@id='tab0']/div[@class='dynamic-input']//text()[contains(.,' parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath(
            "//div[@id='tab0']/div[@class='dynamic-input']//text()[contains(.,'balkon')]").get()
        if balcony:
            if "Geen" in balcony:
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        
        dishwasher = response.xpath(
            "//div[@id='tab0']/div[@class='dynamic-input']//text()[contains(.,'vaatwasser')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        washing = response.xpath("//div[@id='tab1']//strong[.='Aanwezige keukenapparatuur']/following-sibling::p[1]/text()[contains(.,'Wasmachine')]").get()
        if washing:
            item_loader.add_value("washing_machine", True)
        
        
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//ul[@id='image-list']/li/a/img/@src"
            ).extract()
        ]
        if images:
            img = list(set(images))
            item_loader.add_value("images", img)
        
        
        item_loader.add_value("landlord_phone", "013 8894480")
        item_loader.add_value("landlord_name", "The House Key BV")
        item_loader.add_value("landlord_email", "Tilburg@dehuiss Sleutel.nl")

    
        yield item_loader.load_item()

def split_address(address, get):
    city = address.split(",")[1].strip()
    zip_code = address.split(",")[0]
    if get == "zip":
        return zip_code
    else:
        return city