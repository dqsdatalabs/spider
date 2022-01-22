# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'australian_realty_com_au' 
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://australian-realty.com.au/residential-for-lease.html?SalesCategoryID=&Suburbs=&SurroundingArea=&PropertyIDORStreet=&MinPrice=&MaxPrice=&PropertyTypeID=3&Bedrooms=&Bathrooms=&CarSpaces=&orderby=LastModified&history%5B0%5D=%2Fresidential-for-lease.html&act=refresh&offset=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://australian-realty.com.au/residential-for-lease.html?SalesCategoryID=&Suburbs=&SurroundingArea=&PropertyIDORStreet=&MinPrice=&MaxPrice=&PropertyTypeID=2&Bedrooms=&Bathrooms=&CarSpaces=&orderby=LastModified&history%5B0%5D=%2Fresidential-for-lease.html%3FPHPSESSID%3D1266ol7mibjojej83nikebk3u7%26resolution%3D1920%26SalesCategoryID%3D%26Suburbs%3D%26SurroundingArea%3D%26PropertyIDORStreet%3D%26MinPrice%3D%26MaxPrice%3D%26PropertyTypeID%3D3%26Bedrooms%3D%26Bathrooms%3D%26CarSpaces%3D%26orderby%3DLastModified%26offset%3D0&act=refresh&offset=0",
                    "https://australian-realty.com.au/residential-for-lease.html?SalesCategoryID=&Suburbs=&SurroundingArea=&PropertyIDORStreet=&MinPrice=&MaxPrice=&PropertyTypeID=1&Bedrooms=&Bathrooms=&CarSpaces=&orderby=LastModified&history%5B0%5D=%2Fresidential-for-lease.html%3FPHPSESSID%3D1266ol7mibjojej83nikebk3u7%26resolution%3D1920%26SalesCategoryID%3D%26Suburbs%3D%26SurroundingArea%3D%26PropertyIDORStreet%3D%26MinPrice%3D%26MaxPrice%3D%26PropertyTypeID%3D2%26Bedrooms%3D%26Bathrooms%3D%26CarSpaces%3D%26orderby%3DLastModified%26offset%3D0&act=refresh&offset=0",
                    "https://australian-realty.com.au/residential-for-lease.html?SalesCategoryID=&Suburbs=&SurroundingArea=&PropertyIDORStreet=&MinPrice=&MaxPrice=&PropertyTypeID=6&Bedrooms=&Bathrooms=&CarSpaces=&orderby=LastModified&history%5B0%5D=%2Fresidential-for-lease.html%3FPHPSESSID%3D1266ol7mibjojej83nikebk3u7%26resolution%3D1920%26SalesCategoryID%3D%26Suburbs%3D%26SurroundingArea%3D%26PropertyIDORStreet%3D%26MinPrice%3D%26MaxPrice%3D%26PropertyTypeID%3D1%26Bedrooms%3D%26Bathrooms%3D%26CarSpaces%3D%26orderby%3DLastModified%26offset%3D0&act=refresh&offset=0",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'nivoSlider')]/div/a[1]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(.,'>')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Australian_Realty_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h2//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h2[1]/text()", input_type="F_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@id,'mlk')]/text()[contains(.,'Weekly') or contains(.,'weekly') or contains(.,'$')]", input_type="F_XPATH", get_num=True, split_list={" ":0}, replace_list={"$":"", "#":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        rent = " ".join(response.xpath("//div[@id='mlk-51']/text()").getall())
        if rent:
            price = rent.split(" ")[0].replace("$","").strip()
            print(price)
            item_loader.add_value("rent", int(price)*4)



        desc = " ".join(response.xpath("//div[@class='box']/h1//../text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            if "ground" in floor.lower() or "top" in floor.lower() or "first" in floor.lower():
                item_loader.add_value("floor", floor)
                
        item_loader.add_value("external_id", response.url.split("/")[-1].split(".")[0])
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'ROADMAP')]/text()", input_type="F_XPATH", split_list={"'POINT(":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'ROADMAP')]/text()", input_type="F_XPATH", split_list={"'POINT(":1,",":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='box']/h1//../text()[contains(.,'Available')]", input_type="F_XPATH", split_list={"Available":1}, replace_list={":":"", ";":""})
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//img[contains(@src,'bed')]/parent::div/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//img[contains(@src,'bath')]/parent::div/following-sibling::div/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//img[contains(@src,'car')]/parent::div/following-sibling::div/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='imagelightbox']//@href", input_type="M_XPATH")
        landlord_name = " ".join(response.xpath("//div[@class='verticalbox']//a[contains(@class,'label-link')]/text() | //div[@class='verticalbox']/div/div/text()").getall())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        else:
            item_loader.add_xpath("landlord_name", "//div[@class='verticalbox']//div[@id='mlk-34']//text()")
        landlord_phone = response.xpath("//div[@class='verticalbox']//a[contains(@class,'link-clean')]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        else:
            item_loader.add_xpath("landlord_phone", "//div[@class='verticalbox']//a[contains(@href,'tel')]//text()")
        landlord_email = response.xpath("//div[@class='verticalbox']//a[contains(@class,'link-blue')]/@href").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.split(":")[-1].strip())
        else:
            item_loader.add_value("landlord_email", "Sales@australian-realty.com.au")
        yield item_loader.load_item()