# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'studentluxe_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source="Studentluxe_Co_PySpider_united_kingdom"
    start_urls = ["https://www.studentluxe.co.uk/the-collection"]

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[.='View Apartment']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":"apartment"})
        
        for item in response.xpath("//a[.='Click to View']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":"student_apartment"})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response) 
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)

        room_count = " ".join(response.xpath("//div[@class='sqs-block-content']/h4/text()[contains(.,'bedroom')]").extract())
        if room_count:
            room_count=room_count.split("bedroom")[0].replace("Fully furnished","").strip()
            if room_count:
                item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[@class='sqs-block-content']/h3//text()").extract_first()
            if room_count:    
                room_count=room_count.split(" bed")[0].strip()   
                if room_count:
                    item_loader.add_value("room_count", room_count.split(" ")[-1])
                # item_loader.add_value("bathroom_count", room_count.split(",")[1].split("bat")[0].strip())
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room=response.xpath("//div[@class='sqs-block-content']/h2/text()[contains(.,'bed')]").extract_first()
            if room:
                room_count=room.split("bed")[0].strip() 
                if room_count:
                    item_loader.add_value("room_count", room_count.split(" ")[-1])
                if room_count and "one" in room_count.lower():
                    item_loader.add_value("room_count","1")
        roomcheck1=item_loader.get_output_value("room_count")
        if not roomcheck1:
            room1=" ".join(response.xpath("//div[@class='sqs-block-content']/h1//text()").extract())
            if room1:
                room1=room1.split("bed")[0].strip().split(" ")[-1]
                if room1 and "one"==room1:
                    item_loader.add_value("room_count","1")




        bathroom_count = response.xpath("substring-after(//div[@class='sqs-block-content']/h4/text()[contains(.,'bathroom')],', ')").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("bathroom")[0].strip())
        else:
            bathroom_count = response.xpath("substring-after(//h3//text()[contains(.,'bath')],', ')").extract_first()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split("bath")[0].strip())

        meters = response.xpath("substring-after(//div[@class='sqs-block-content']/h4/text()[contains(.,' square metres')],'| ')").extract_first()
        if meters:
            item_loader.add_value("square_meters", meters.split("square")[0].strip())

        rent = "".join(response.xpath("//div[1]/div[@class='sqs-block-content']/h2/strong[1]/text()").getall())
        price = ""
        if rent:
            if "per week" in rent:
                price = rent.split("per week")[0].split("£")[1].strip().replace(",","")
        else:
            rent = "".join(response.xpath("//div[1]/div[@class='sqs-block-content']/h1//strong/text()").getall())
            if rent:
                if "per week" in rent:
                    price = rent.split("per week")[0].split("£")[1].strip().replace(",","")
        if price.isdigit():
            item_loader.add_value("rent", str(int(float(price))*4)) 
        item_loader.add_value("currency", "GBP")
        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            rent=response.xpath("//div[@class='sqs-block-content']/h2/text()").get()
            if rent:
                rent=rent.split("per")[0].split("£")[-1]
                item_loader.add_value("rent",rent)

        description = " ".join(response.xpath("//div[@class='sqs-block-content']/p/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        address = " ".join(response.xpath("//div[contains(@class,'col sqs-col-8')]//div[@class='sqs-block-content']/h3/strong/text()[not(contains(.,'bath'))]").getall()).strip()   
        if address:
            item_loader.add_value("address", address)
        else:
            item_loader.add_xpath("address", "substring-after(//div[@class='sqs-block-content']/h3/text(),'by ')")

        latlng = response.xpath("//div[@class='sqs-block map-block sqs-block-map']/@data-block-json").extract_first()
        if latlng:
            jseb = json.loads(latlng)
            lat = jseb["location"]["mapLat"]
            lng = jseb["location"]["mapLng"]
            item_loader.add_value("latitude", str(lat))
            item_loader.add_value("longitude", str(lng))

        images = [ x for x in response.xpath("//div[@class='gallery-reel-item-src']//img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        else:
            images = [ x for x in response.xpath("//div[@class='gallery-fullscreen-slideshow-item-img']//img/@data-src").getall()]
            if images:
                item_loader.add_value("images", images)

        furnished = response.xpath("//h4[contains(.,'Furnished') or contains(.,'furnished') ]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        terrace = response.xpath("//h3[contains(.,'Terrace') or contains(.,'terrace') ]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "Student Luxe")
        item_loader.add_value("landlord_phone", "+44 7790 664737")
        item_loader.add_value("landlord_email", "HELLO@STUDENTLUXE.CO.UK")
            
        

        
        yield item_loader.load_item()
