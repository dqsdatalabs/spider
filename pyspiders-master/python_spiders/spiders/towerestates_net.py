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

class MySpider(Spider):
    name = 'towerestates_net'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source = "Towerestates_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.towerestates.net/property-search/?real_location=&real_type=flat&real_status=for-rent&real_bedroom=any&real_bathroom=any&min_price=any&max_price=any",
                    "http://www.towerestates.net/property-search/?real_location=&real_type=apartment&real_status=for-rent&real_bedroom=any&real_bathroom=any&min_price=any&max_price=any"
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.towerestates.net/property-search/?real_location=&real_type=detached-house&real_status=for-rent&real_bedroom=any&real_bathroom=any&min_price=any&max_price=any",
                    "http://www.towerestates.net/property-search/?real_location=&real_type=house&real_status=for-rent&real_bedroom=any&real_bathroom=any&min_price=any&max_price=any",
                    "http://www.towerestates.net/property-search/?real_location=&real_type=maisonette&real_status=for-rent&real_bedroom=any&real_bathroom=any&min_price=any&max_price=any",
                    "http://www.towerestates.net/property-search/?real_location=&real_type=terraced-house&real_status=for-rent&real_bedroom=any&real_bathroom=any&min_price=any&max_price=any",
                    "http://www.towerestates.net/property-search/?real_location=&real_type=house-share&real_status=for-rent&real_bedroom=any&real_bathroom=any&min_price=any&max_price=any"
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://www.towerestates.net/property-search/?real_location=&real_type=studio&real_status=for-rent&real_bedroom=any&real_bathroom=any&min_price=any&max_price=any",
                ],
                "property_type" : "studio",
            },
            {
                "url" : [
                    "http://www.towerestates.net/property-search/?real_location=&real_type=room&real_status=for-rent&real_bedroom=any&real_bathroom=any&min_price=any&max_price=any",
                ],
                "property_type" : "room",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='listing-title']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})      
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title/text()")

        address = "".join(response.xpath("//p[@class='single-property-address']/text()").getall())
        if address:
            city = ""
            zipcode = ""
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            city_list = ["london","orpington","enfield","stoke newington","wood green", "winchmore hill"]
            try:
                for x in city_list:
                    if x in address.lower():
                        city = x
                        break
                if city:
                    zipcode = address.upper().split(city.upper())[-1]
                    zipcode = zipcode.replace(", UK","").split(",")[-1]
                else:
                    zipcode = " ".join(address.strip().split(",")[-1].split(" ")[-2:])
                if city:
                    item_loader.add_value("city", city.capitalize())
                else:
                    city1 = response.xpath("//h3[contains(@class,'property-title')]//text()").get()
                    for x in city_list:
                        if x in city1.lower():
                            city = x
                            break
                    if city:
                        item_loader.add_value("city", city.capitalize())
                if not zipcode.replace(" ","").isalpha():
                    item_loader.add_value("zipcode", zipcode.strip())
            except:
                pass
        rent = "".join(response.xpath("//div[@class='single-property-price']/h3/text()").getall())
        if rent:
            price = rent.replace(",",".").replace(".","")
            item_loader.add_value("rent",price.strip())
        item_loader.add_value("currency","GBP")

        room_count = "".join(response.xpath("//span[@class='meta-bedroom']/text()").getall())
        if room_count:
            if room_count !="0":
                item_loader.add_value("room_count",room_count)
            elif response.meta.get('property_type') == "studio":
                item_loader.add_value("room_count","1")
        bathroom_count = "".join(response.xpath("//span[@class='meta-bathroom']/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        description = " ".join(response.xpath("//div[@class='single-property-content-wrapper']/div/h2[.='Full Description']/following-sibling::p/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())
        else:
            description = " ".join(response.xpath("//div[@class='single-property-content']/p//text()").getall()).strip()   
            if description:
                item_loader.add_value("description", description.strip())

        images = [response.urljoin(x) for x in response.xpath("//li[@data-target='#myCarousel']/img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)

        floor = "".join(response.xpath("//div[@class='single-property-content-wrapper']/div/h2[.='Key Features']/following-sibling::ul/li/text()[contains(.,'Floor')]").getall())
        if floor:
            floor = floor.split(" ")[0].strip()

        LatLng = "".join(response.xpath("substring-before(substring-after(//script/text()[contains(.,'var property_location')],'lat'),',')").getall())
        if LatLng:
            item_loader.add_value("latitude",LatLng.replace('":"',"").replace('"',"").strip())
            lng = "".join(response.xpath("substring-before(substring-after(//script/text()[contains(.,'var property_location')],'lng'),'}')").getall())
            item_loader.add_value("longitude",lng.replace('":"',"").replace('"',"").strip())

        parking = "".join(response.xpath("//span[@class='meta-garage']/text()").getall())
        if parking:
            if parking !="0":
                item_loader.add_value("parking",True)
            elif parking == "0":
                item_loader.add_value("parking",False)

        furnished = "".join(response.xpath("//div[@class='single-property-content-wrapper']/div/h2[.='Key Features']/following-sibling::ul/li/text()").getall())
        if furnished:
            if "Unfurnished" in furnished :
                item_loader.add_value("furnished",False)
            elif "Furnished" in furnished:
                item_loader.add_value("furnished",True)

        item_loader.add_value("landlord_phone", "020 8920 9820")
        item_loader.add_value("landlord_email", "towerestates1@gmail.com")
        item_loader.add_value("landlord_name", "Tower Estates")

        
        yield item_loader.load_item()