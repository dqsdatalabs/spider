# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags 
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re

class MySpider(Spider):
    name = 'goviewlondon_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.goviewlondon.co.uk/search/?address_keyword_exact=1&showsold=on&showstc=on&instruction_type=Letting&minprice=&property_type%5B%5D=Apartment%2CBedsit%2CFlat%2CGround+Floor+Flat%2CGround+Floor+Maisonette%2CPenthouse%2CShared+House%2CStudio&address_keyword=&maxprice=&n=25",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.goviewlondon.co.uk/search/?address_keyword_exact=1&showsold=on&showstc=on&instruction_type=Letting&minprice=&property_type%5B%5D=Detached+House%2CEnd+Terraced+House%2CMid+Terraced+House%2CSemi-Detached+House%2CTown+House&address_keyword=&maxprice=&n=25",
                "property_type" : "house"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'property')]/div"):
            f_url = response.urljoin(item.xpath(".//div/a[contains(.,'MORE DETAILS')]/@href").get())
            room_count=item.xpath("./div/ul/li/img[@alt='bedrooms']/parent::li/text()").get()
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type"),"room_count": room_count},
            )
        
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")},
            )
          
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        let = response.xpath("//div[@id='property-carousel']/img/@src[contains(.,'let')]").extract_first()
        if let:
            return
            
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Goviewlondon_PySpider_"+ self.country + "_" + self.locale)
        externalid=response.url
        if externalid: 
            externalid=externalid.split("details/")[-1].split("/")[0]
            item_loader.add_value("external_id",externalid)
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        address=response.xpath("//head/title/text()").get()
        if address:
            item_loader.add_value("address", address)
        item_loader.add_value("city", "London")

        zipcode = response.xpath("substring-after(//div[@class='address']//span[@itemprop='name']/text(),', ')").extract_first()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

            
        rent=response.xpath("//span[@class='price']/strong/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",",""))
            
        desc="".join(response.xpath("//div/h1[contains(.,'Property Details')]/parent::div/following-sibling::div//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}','',desc))
        if ("furnished" in desc.lower()) and ("unfurnished" not in desc.lower()):
            item_loader.add_value("furnished", True)
        if "balcony" in desc.lower():
            item_loader.add_value("balcony", True)
        if "terrace" in desc.lower():
            item_loader.add_value("terrace", True)
        if "studio" in desc:
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_value("room_count", response.meta.get('room_count'))

        from python_spiders.helper import ItemClear
        if not item_loader.get_collected_values("room_count"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'BEDROOM')]/text()", input_type="F_XPATH", get_num=True, split_list={"BEDROOM":0})
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'BATHROOM')]/text()", input_type="F_XPATH", get_num=True, split_list={"BATHROOM":0})

        parking = "".join(response.xpath("//ul/li[contains(.,'PARKING')]/text()").getall())
        if parking:
            item_loader.add_value("parking", True)
            
        images=[x for x in response.xpath("//img[@class='img-responsive img-thumbnail']/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name", "GOVIEW LONDON")
        phone=response.xpath("//a/@href[contains(.,'Tel')]/parent::a/strong/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        item_loader.add_value("landlord_email", "lettings@goviewlondon.co.uk")
        
        yield item_loader.load_item()

