# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from re import S
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'remaxelite_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source="Remaxelite_Com_PySpider_australia"
  
 
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.macarthurrealestate.com.au/lease/for-lease/?list=lease&keywords=&property_type%5B%5D=House&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.macarthurrealestate.com.au/lease/for-lease/?list=lease&keywords=&property_type%5B%5D=Townhouse&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.macarthurrealestate.com.au/lease/for-lease/?list=lease&keywords=&property_type%5B%5D=Semi+Detached&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.macarthurrealestate.com.au/lease/for-lease/?list=lease&keywords=&property_type%5B%5D=Unit&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='embed-responsive-item']//@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item,meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source) 
        item_loader.add_value("property_type",response.meta.get("property_type"))

        title=response.xpath("//h2[@class='property-address']/text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//h2[@class='property-address']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        zipcode=response.xpath("//h2[@class='property-address']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip().split(" ")[-1])
        external_id =response.url
        if external_id:
            item_loader.add_value("external_id", external_id.split("/")[-1]) 
        rent = response.xpath("//label[.='Price']/following-sibling::div/text()").get()
        if rent:
            if "Weekly" in rent:
                rent = rent.split("$")[-1].split("Weekly")[0].strip().replace(',', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'USD')
            else:       
                item_loader.add_value("rent_string", rent.replace(",","").replace(" ",""))
        available_date = response.xpath("//label[.='Available Date']/following-sibling::div/text()").get()
        if available_date:
            available_date=available_date.split(",")[-1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        description = " ".join(response.xpath("//div[@class='detail-description mb-3']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        images = [x for x in response.xpath("//picture//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        room_count=response.xpath("//i[@class='las la-bed']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//i[@class='las la-bath']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//i[@class='las la-car']/following-sibling::span/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        pets_allowed=response.xpath("//h5[.='Features']/following-sibling::div//li//text()").getall()
        for i in pets_allowed:
            if "Pet" in i:
                item_loader.add_value("pets_allowed",True)
        square_meters=response.xpath("//label[.='Land Size']/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("sqm")[0].strip())


        name=response.xpath("//div[@class='agent-detail w-30']/p[@class='name mb-0']/strong/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//div[@class='agent-detail w-30']/p[@class='phone mb-0']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[-1])
        email=response.xpath("//div[@class='agent-detail w-30']/p[@class='email mb-0']/text()").get()
        if email:
            item_loader.add_value("landlord_email",email.split(":")[-1])

        yield item_loader.load_item()

