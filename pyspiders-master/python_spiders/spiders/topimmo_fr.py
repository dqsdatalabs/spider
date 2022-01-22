# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser

class MySpider(Spider):
    name = 'topimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Topimmo_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.topimmo.fr/nos-annonces/location/?sort=date-desc&status=location&property_type=appartement&city=&lat=&lng=&distance=&ref=&rooms=&bedrooms=&minBudget=&maxBudget=&minSurface=&maxSurface=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.topimmo.fr/nos-annonces/location/?sort=date-desc&status=location&property_type=maison&city=&lat=&lng=&distance=&ref=&rooms=&bedrooms=&minBudget=&maxBudget=&minSurface=&maxSurface=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base": item})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='cntbtn']//a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//div[@class='cntpaged']/a[@class='next page-numbers']/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//div[@class='item-left']//h1/text()")  
        external_id = response.xpath("substring-after(//div[@class='cnt-ref']/text(),'Ref. ')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        room_count = response.xpath("//p[text()='Chambres : ']/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//p[text()='Pièces : ']/span/text()")
     
        item_loader.add_xpath("floor", "//p[text()='Etage : ']/span/text()")
 
        energy_label = response.xpath("//script[contains(.,'.dpe(') and contains(.,'value:')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("value:")[1].split(",")[0])
        address = response.xpath("//div[@class='cntcity']/span/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[-1].split(")")[0].strip())  

        square_meters = response.xpath("//p[text()='Surface : ']/span/text()").get()
        if square_meters:
            square_meters=' '.join(square_meters.strip().split(" ")[:1]).replace(".",",")
            item_loader.add_value("square_meters", square_meters)
       
        description = " ".join(response.xpath("//div[@class='cnttxt']/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        elevator = response.xpath("//p[text()='Ascenseur : ']/span/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        images = [x for x in response.xpath("//div[@id='mainGallery']//div[@class='boxGallery']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
 
        rent = response.xpath("//div[@class='cntprice']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ","").replace("\xa0",""))
        else:
            item_loader.add_value("currency","EUR")
       
        utilities = response.xpath("//div[@class='boxprice']//text()[contains(.,'Charges mensuelles')]/following-sibling::span[1]/text()").get()
        if utilities:
            utilities = utilities.replace("€","").strip()
            item_loader.add_value("utilities", utilities)
            
        latitude_longitude = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if latitude_longitude:
            latitude_longitude = latitude_longitude.split('L.marker([')[1].split(']')[0]
            item_loader.add_value("longitude", latitude_longitude.split(",")[0])
            item_loader.add_value("latitude", latitude_longitude.split(",")[1])

        landlord_name = response.xpath("//div[@id='sectPhone']/div[@class='box-aside']/h3/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        item_loader.add_xpath("landlord_phone", "substring-after(//div[@id='sectPhone']//a[contains(@href,'tel')]/@href,':')")
        item_loader.add_value("landlord_email", "referencementprestataire@gmail.com")
       
        yield item_loader.load_item()