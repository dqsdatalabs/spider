# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'placewest_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source = "Placewest_PySpider_australia"

    def start_requests(self):
        start_urls = [
	        {
                "url": [
                    "https://placewest.com.au/residential/lease"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='text-wrapper']//a[@class='list']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id = "".join(response.url)
        if external_id:
            external_id = external_id.split("/")[-2:-1]
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title.replace("\u00e9","").replace("\u00e8","").replace("\u00b2","").replace("\u00a0",""))
            zipcode = title.split("QLD")[1]
            if zipcode:
                item_loader.add_value("zipcode", zipcode)
            address= title.split("Place West ::")[1] 
            if address:
                item_loader.add_value("address", address)
            city= title.split(",")[-2:-1] 
            if city:
                item_loader.add_value("city", city)

        description = "".join(response.xpath("//div[@class='text']//span//text()").getall())
        if description:
            item_loader.add_value("description", description.replace("\u00e9","").replace("\u00e8","").replace("\u00b2","").replace("\u00a0","").replace("\u0153","").replace("\u00e0",""))

        room_count = response.xpath("//span[@class='bed']//h1//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)  
       
        bathroom_count = response.xpath("//span[@class='bath']//h1//text()").get()
        if bathroom_count :
            item_loader.add_value("bathroom_count", bathroom_count)

        price = response.xpath("//span[@class='price']/text()").get()
        if price and ("week" in price.lower() or "p/w" in price.lower()):
            price= price.split("$")[1].split(" ")[0]
            if "," in price:
                price = price.replace(",","") 
                item_loader.add_value("rent", int(price)*4)
            else:
                item_loader.add_value("rent", int(price)*4)
        else:
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "AUD")

        deposit = response.xpath("//span[contains(.,'Bond')]/parent::td/following-sibling::td/span/text()").get()
        if deposit:
            deposit= deposit.split("$")[1].replace(",","").replace(".","") 
            if "," in deposit:
                item_loader.add_value("deposit", deposit)
            else:
                item_loader.add_value("deposit", deposit)

        latitude_longitude = response.xpath(
            "//script[contains(.,'latLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                '{lat: ')[1].split(',')[0]
            longitude = latitude_longitude.split(
                'lng: ')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        available_date = response.xpath("(//tr[@class='available']//span//text())[2]").get()
        if available_date and not "now" in available_date.lower():
            available_date = available_date.split(",")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        parking = response.xpath("//span[@class='car']/h1/text()").get()
        if parking:
            if int(parking) > 0:
                item_loader.add_value("parking", True)

        images = [x for x in response.xpath("//a[@rel='colorbox-img']//@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
  
        landlord_name = response.xpath("//div[@class='agents']//div[@class='bottom']//h1//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name),

        landlord_phone = response.xpath("//div[@class='agents']//div[@class='bottom']//h2//text()[contains(.,'0')]").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
                    
        landlord_email = response.xpath("//div[@class='agents']//div[@class='bottom']//h2//text()[contains(.,'@')]").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        yield item_loader.load_item()