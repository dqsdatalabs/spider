# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
#


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'ajpimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.ajp-immobilier.com/recherche?property=appartement&listing_type=rent&page=1", "property_type": "apartment"},
	        {"url": "https://www.ajp-immobilier.com/recherche?property=maison&listing_type=rent&page=1", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):

        for follow_url in response.xpath("//div[contains(@class,'property-list__item')]//@href[contains(.,'annonces')]").extract():
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get("property_type")})
        
        pagination = response.xpath("//ul[@class='pagination']/li[contains(.,'Next')]/a/@href").extract_first()
        if pagination:
            yield Request(pagination, callback=self.parse, meta={"property_type": response.meta.get("property_type")})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Ajpimmobilier_PySpider_"+ self.country + "_" + self.locale)
        
        title = response.xpath("normalize-space(//h2[@id='property-details__title-text']/text())").extract_first()
        item_loader.add_value("title", title)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.xpath("//p[contains(.,'REF')]/text()").get().split("-")[0].strip())
   
        description = "".join(response.xpath("//h3[contains(.,'Description')]/following-sibling::p[1]/text()").extract())
        if description:
            item_loader.add_value("description", description)
        
        price = "".join(response.xpath("//div[@id='property-details__images-price']/text()").extract())
        if price:
            rent = price.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        
        if "de garantie" in description:
            deposit=description.split("garantie")[1].replace(":","").strip().split(" ")[0]
            if "." in deposit:
                deposit = deposit.split(".")[0]
            else:
                deposit = deposit.lower().replace("euros","").replace("eur","").replace(",","").strip()
            if deposit.isdigit():
                if deposit < "10":
                    deposit = int(rent.replace(" ",""))*int(deposit.replace(" ",""))
                item_loader.add_value("deposit", deposit)
        
        address = response.xpath("//p[contains(.,'REF')]/text()").get()
        if address:
            address = address.split("-")[1].replace("AJP","").replace("Immobilier","").strip()
            item_loader.add_value("address", address)

        item_loader.add_value("property_type", response.meta.get("property_type"))
        
        # square_meters = response.xpath("//img[contains(@src,'dimensions')]/following-sibling::span/text()").get()
        # if square_meters:
        #     print(square_meters)
            # square_meters = square_meters.split("m²")[0].strip()
            # if square_meters != "0": 
            #     item_loader.add_value("square_meters", square_meters)
            # elif sq_m != "0":
            #     item_loader.add_value("square_meters", sq_m)
                
        room_count = response.xpath("//div[@class='property-detail col-auto mb-2'][3]/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room = response.xpath("//img[contains(@src,'rooms-icon')]/following-sibling::span/text()[.!='0']").get()
            if room:
                item_loader.add_value("room_count", room)
        
        bathroom = response.xpath("//div[@class='property-detail col-auto mb-2'][5]/span/text()").get()
        if bathroom != "0":
            item_loader.add_value("bathroom_count", bathroom)
        
        images = [x for x in response.xpath("//div[@class='carousel-inner']/div/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//div[@class='property-detail col-auto mb-2'][1]/span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)

        lat_lng=response.xpath("//script[contains(.,'coordinates')]/text()").get()
        if lat_lng:
            lat=lat_lng.split('coordinates":[[')[1].split(",")[0]
            lng=lat_lng.split('coordinates":[[')[1].split(",")[1].split("]")[0]
            if lat and lng:
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)
        
        energy_label = response.xpath("//p[@class='consumption-details mb-2']/span/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        landlord_name = response.xpath("//div[@id='property-details__form-agency-details']/p[contains(.,'AJP')]/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//div[@id='property-details__form-agency-details']/p[@class='phone']/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        
        landlord_email = response.xpath("//div[@id='property-details__form-agency-details']/p[contains(.,'@')]/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()