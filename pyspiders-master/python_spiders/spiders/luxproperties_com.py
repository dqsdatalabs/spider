# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import Coroutine
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'luxproperties_com'
    start_urls = ['https://luxproperties.com.tr/action/kiralik/']  # LEVEL 1

    execution_type = 'testing'
    country = 'turkey'
    locale = 'tr'
    # 1. FOLLOWING

    s = {}
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='carousel-inner']/div[1]/a/@href").extract():
            yield Request(item, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://luxproperties.com.tr/action/kiralik/page/{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        room=response.xpath("//div[contains(@id,'collapseOne')]/div/div[contains(.,'Oda')]/text()").extract()
        commercial = response.xpath("//div[@class='wpestate_property_description'][contains(.,'Ofis')]//text()").extract_first()
        if commercial:
            return
        title = response.xpath("//h1[contains(@class,'entry-title')]/text()").get()
        item_loader.add_value("title", title)
        item_loader.add_value("external_link",response.url)
        item_loader.add_value("external_source", "Luxproperties_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_xpath("external_id", "//div[contains(@id,'collapseOne')]/div/div[contains(.,'Emlak ID')]/text()")    
        item_loader.add_xpath("description","//div[@class=('wpestate_property_description')]/p/text()")
        item_loader.add_xpath("city","//div[contains(@id,'collapseTwo')]/div/div[contains(.,'İlçe')]/a/text()")
        item_loader.add_xpath("zipcode","//div[contains(@id,'collapseTwo')]/div/div[contains(.,'Posta')]/text()")

        property_type = response.xpath("//div[@class=('property_categs')]/a[1]/text()").get()
        if property_type and ("Residence" or "Konut") in property_type:
            item_loader.add_value("property_type","apartment")
        elif property_type and "Villa" in property_type:
            item_loader.add_value("property_type","house")
        else:
            return
            
        city=response.xpath("//div[contains(@id,'collapseTwo')]/div/div[contains(.,'İlçe')]/a/text()").extract_first()
        # country=response.xpath("//div[contains(@id,'collapseTwo')]/div/div[contains(.,'Ülke')]/a/text()").extract_first()
        district=response.xpath("//div[contains(@id,'collapseTwo')]/div/div[contains(.,'Semt')]/a/text()").extract_first()
        address=str(district)+" "+str(city)
        if address:
            item_loader.add_value("address",address)
            
        square_meters=response.xpath("//div[contains(@id,'collapseOne')]/div/div[contains(.,'Emlak m²') and not(contains(.,'Brüt'))]/text()").extract_first()
        
        if square_meters is not None:
            item_loader.add_value("square_meters",square_meters.strip("m").strip())
        # else:
        #     square_meters=response.xpath("//div[contains(@id,'collapseOne')]/div/div[contains(.,'Emlak Brüt m²')]/text()").extract_first()
        #     if square_meters is not None:
        #         item_loader.add_value("square_meters",square_meters.strip("m"))
                
        room=response.xpath("//div[contains(@id,'collapseOne')]/div/div[contains(.,'Yatak Oda')]/text()").extract()
        if len(room)>=1 :
            room_count=0
            for i in  range(0,len(room)):
                room_count+=int(room[i])
            item_loader.add_value("room_count",str(room_count))
        
        bathroom_count=response.xpath("//div[contains(@id,'collapseOne')]/div/div[contains(.,'Banyo')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
            
        images = [response.urljoin(x)for x in response.xpath("//ol[contains(@class,'carousel-indicators')]/li/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
            
        price = response.xpath("//span[@class='price_area']/text()").get()
        if price:
            price = price.strip().replace(",","")
            item_loader.add_value("rent",price)
            item_loader.add_value("currency", "TRY")
        floor=response.xpath("//div[contains(@id,'collapseOne')]/div/div[contains(.,'Kat')]/text()").extract_first()
        if floor is not None:
            if "Bahçe" in floor.strip():
                item_loader.add_value("floor","0")
            else:    
                item_loader.add_value("floor",floor.strip())

        parking=response.xpath("//div[@id='collapseThree']/div/div[text()[contains(.,'Otopark')]]").extract_first()
        if parking:
            item_loader.add_value("parking",True)
        
        elevator=response.xpath("//div[@id='collapseThree']/div/div[text()[contains(.,'Asansör')]]").extract_first()
        if elevator:
            item_loader.add_value("elevator",True)
        
        balcony=response.xpath("//div[@id='collapseThree']/div/div[text()[contains(.,'Balkon')]]").extract_first()
        if balcony:
            item_loader.add_value("balcony",True)

        item_loader.add_xpath("latitude","//div[@id='googleMap_shortcode']/@data-cur_lat")
        item_loader.add_xpath("longitude","//div[@id='googleMap_shortcode']/@data-cur_long")
        
        item_loader.add_value("landlord_name","Lux Properties")
        item_loader.add_value("landlord_email","info@lp.com.tr")
        item_loader.add_value("landlord_phone","444 2 071")

        yield item_loader.load_item()