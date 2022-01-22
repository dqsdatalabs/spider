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
import re


class MySpider(Spider):
    name = "dermul_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    external_source='Dermul_PySpider_belgium_nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.dermul.be/nl/te-huur/lijst?type%5B%5D=5096&selectItemtype%5B%5D=5096&price%5Bmin%5D=&price%5Bmax%5D=&bedrooms%5Bmin%5D=&bedrooms%5Bmax%5D=&street=&ref=&sort_bef_combine=field_price_value+ASC",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.dermul.be/nl/te-huur/lijst?type%5B%5D=5100&selectItemtype%5B%5D=5100&price%5Bmin%5D=&price%5Bmax%5D=&bedrooms%5Bmin%5D=&bedrooms%5Bmax%5D=&street=&ref=&sort_bef_combine=field_price_value+ASC",
                "property_type" : "house"
            },
        ]# LEVEL 1
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath(
            "//div[@class='search-view-results']/div[@class='view-content']/div[not (contains(@class,'block'))]//div[contains(@class,'views-field-field-subtitle')]//a/@href"
        ).extract():
            yield Request(item, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[.='›']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Dermul_PySpider_" + self.country + "_" + self.locale)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        square = response.xpath(
            "//div[@class='field field-name-field-livable-surface']/text()[ .!='0m²']"
        ).get()
        room_count = response.xpath("//div[@class='field field-name-field-bedrooms']/text()[.!=0]").extract_first()
        room_studio = response.xpath("//div[@class='field field-name-field-description']//text()[contains(.,'studio') or contains(.,'Studio')]").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count )
        if not room_count and room_studio:
            item_loader.add_value("room_count","1" ) 
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room1= "".join(response.xpath("//div[@class='field field-name-field-description']//text()").extract())
            roomindex=room1.find("slaapkamer")
            roomcount=re.findall("\d+",room1[:roomindex])
            item_loader.add_value("room_count",roomcount)
        
        
        desc = "".join(response.xpath("//div[@class='field field-name-field-description']//text()").extract())
        item_loader.add_value(
            "description", desc.strip()
        )

        rent = response.xpath(
            "//div[@class='field field-name-field-price']/text()"
        ).get()
        if rent:
            item_loader.add_value("rent", rent.split(" ")[1])
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("external_link", response.url)

        if square:
            square = square.split("m²")[0]
            item_loader.add_value("square_meters", square)
        # item_loader.add_xpath(
        #     "property_type",
        #     "//div[@class='field field-name-field-maximmo-type']/text()",
        # )
        prop_type = response.xpath("//div[contains(@class,'properties')]//div[contains(.,'Type')]/parent::div/text()").get()
        if "Studio" in prop_type:
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        date = response.xpath(
            "//div[@class='field-items']/div[contains(@class,'field-item')][1]/text()[contains(.,'/') and not(contains(.,'20201'))]"
        ).get()
        if date:
            # dates = date.strip().replace().split(' ')
            match = re.search(r'(\d+/\d+/\d+)',date)
            if match:
                new_format = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
            item_loader.add_value(
                "available_date",new_format 
            )

        floor = response.xpath(
            "//div[@class='field-items']/div[contains(.,'verdieping')]/text()"
        ).get()
        if floor:
            item_loader.add_value("floor", floor.split("verdieping")[0])

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[contains(@class,'field-name-field-images')]//a/@href"
            ).extract()
        ]
        item_loader.add_value("images", images)

        energy_label = response.xpath("//div[contains(@class,'field-name-field-maximmo-epc')]/text()").get()
        energy = response.xpath("//div[contains(@class,'field-name-field-epc')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        elif energy and energy.isdigit():
            item_loader.add_value("energy_label", energy_label_calculate(energy))
            
        address = " ".join(response.xpath(
            "//div[@class='field field-name-field-maximmo-address']//text()"
        ).extract())
        if address:
            item_loader.add_value("address", address)
        item_loader.add_xpath("zipcode", "//span[@class='postal-code']/text()")
        item_loader.add_xpath("city", "//span[@class='locality']/text()")

        terrace = response.xpath(
            "//div[@class='field-items']/div[contains(.,'terras')]"
        ).get()
        if terrace:
            item_loader.add_value("terrace", True)

        elevator = response.xpath(
            "//div[contains(@class,'field-item ')][contains(.,'lift')]"
        ).get()
        if elevator:
            if "geen" in elevator:
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)

        balcony = response.xpath(
            "//div[@class='field field-name-field-description']//text()[contains(.,'balkon')]"
        ).get()
        if balcony:
            item_loader.add_value("balcony", False)

        
        washingmachine = response.xpath(
            "//div[@class='field field-name-field-description']//text()[contains(.,'wasmachine')]"
        ).get()
        if washingmachine:
            item_loader.add_value("washing_machine", True)
        
        parking = response.xpath(
            "//div[@class='field field-name-field-description']//text()[contains(.,'parkeren') or contains(.,'garage')]"
        ).get()
        if parking:
            item_loader.add_value("parking", True)    

        item_loader.add_value("landlord_name", "Agence Dermul")
        item_loader.add_value("landlord_phone", "059 55 10 50")
        item_loader.add_value("landlord_email", "info@dermul.be")

        heating_cost = response.xpath("//div[@class='field-items']/div[contains(.,'verwarming')]/text()").get()
        if heating_cost:
            try:
                heating_cost = heating_cost.split("€")[1].strip().split(" ")[0]
            except:
                heating_cost = heating_cost.split("verwarming ")[0].split("e")[1].strip()
                
            item_loader.add_value("heating_cost", heating_cost)
    
        water_cost = response.xpath("//div[@class='field-items']/div[contains(.,'water')]/text()").get()
        if water_cost:
            try:
                water_cost = water_cost.split("€")[1].strip().split(" ")[0]
            except:
                water_cost = water_cost.split("verwarming ")[0].split("e")[1].strip()
            item_loader.add_value("water_cost", water_cost)

        yield item_loader.load_item()

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