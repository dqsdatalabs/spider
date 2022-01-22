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
    name = 'avanabuilds_com' 
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source="Avanabuilds_PySpider_canada"

    # 1. FOLLOWING
    def start_requests(self):
        url =  "https://avanabuilds.com/rent-with-us/"
        yield Request(url,callback=self.parse)
 
    def parse(self,response):
        
        formdata = {
            "view": "listing",
            "action": "de_li_filter",
            "paged": "1",
            "min": "",
            "max": "",
            "type": "",
            "city": "",
            "bed": "",
            "pet":"" ,
        }
        yield FormRequest(
            "https://avanabuilds.com/wp/wp-admin/admin-ajax.php",
            formdata=formdata,
            callback= self.parse_list,
            dont_filter=True


        )
    def parse_list(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data=json.loads(response.body)['listings']
        list=Selector(text=data).xpath("//div[@class='thumbnail']/preceding-sibling::a/@href").getall()
        for url in list: 
            follow_url = url
            yield Request(follow_url,callback=self.populate_item)
            seen = True
        if page == 2 or seen:
            formdata = {
                "view": "listing",
                "action": "de_li_filter",
                "paged": str(page),
                "min": "",
                "max": "",
                "type": "",
                "city": "",
                "bed": "",
                "pet":"" ,
            }
            url = "https://avanabuilds.com/wp/wp-admin/admin-ajax.php"
            yield FormRequest(
                url,
                formdata = formdata,
                method = "POST",
                callback = self.parse_list,
                meta={"page": page+1}
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        address=title
        if address:
            item_loader.add_value("address",address.split("-")[0].strip())
        rent=response.xpath("//h3[contains(.,'month')]/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("$")[1].split("/month")[0])
        item_loader.add_value("currency","USD")
        try:
            square_meters=response.xpath("//div[contains(.,'sq.ft')]/text()").get()
            if square_meters:
                item_loader.add_value("square_meters",square_meters.split("sq.ft")[0].strip())
        except: 
            pass
        description="".join(response.xpath("//div[@class='elementor-widget-container']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//div/figure/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("property_type","house")
        features=response.xpath("//h3[.='Features of this suite include:']/parent::div/parent::div/following-sibling::div//li//text()").getall()
        if features:
            for i in features:
                if "bathroom" in i.lower(): 
                    if "one" in i.lower():
                        item_loader.add_value("bathroom_count","1")
                    if "two" in i.lower():
                        item_loader.add_value("bathroom_count","2")
                    if "three" in i.lower():
                        item_loader.add_value("bathroom_count","3")
                if "bedroom" in i.lower(): 
                    if "one" in i.lower():
                        item_loader.add_value("room_count","1")
                    if "two" in i.lower():
                        item_loader.add_value("room_count","2")
                    if "three" in i.lower():
                        item_loader.add_value("room_count","3")
                if "parking" in i.lower():
                    item_loader.add_value("parking",True)
                if "pet-friendly" in i.lower():
                    item_loader.add_value("pets_allowed",True)
                if "yard" in i.lower():
                    item_loader.add_value("terrace",True)
        item_loader.add_value("landlord_name","AVANA")
        phone=response.xpath("//a[contains(@href,'tel')]/@href").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split("tel:")[-1])
        email=response.xpath("//a[contains(@href,'mail')]/@href").get()
        if email:
            item_loader.add_value("landlord_email",email.split("mailto:")[-1])


                    

        yield item_loader.load_item()