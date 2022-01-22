# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'hkemlak_com'
    execution_type='testing'
    country='turkey'
    locale='tr'   
    start_urls = ["http://www.hkemlak.com/?durumu=kiralik&cat=2"]
    
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='propertyImgLink']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[.='»']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )  

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rented = response.xpath("//h1/text()[contains(.,'SATILIK ')]").extract_first()
        if rented :
            return
        item_loader.add_value("external_link", response.url)
        prop_type = "".join(response.xpath("//div[@class='col-lg-8']/h1[1]/text()").get())
        if prop_type and "DAİRE" in prop_type:
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "EV" in prop_type:
            item_loader.add_value("property_type", "house")
        else:
            return
               
        item_loader.add_value("external_source", "Hkemlak_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_xpath("external_id","//ul[@class='overviewList']/li[contains(.,'İlan No')]/span//text()")

        item_loader.add_xpath("title","//div[@class='row']//h1/text()")
        item_loader.add_xpath("rent_string","//ul[@class='overviewList']/li[contains(.,'Fiyat')]/span//text()")
      
        square_meters = response.xpath("//ul[@class='overviewList']/li[contains(.,'Metrekare')]/span//text()").extract_first()
        if square_meters :
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='single_slider']//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)      

        description="".join(response.xpath("//p[contains(@class,'price')]//parent::div[contains(@class,'col-lg-8')]//p//text()").extract())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description",description.strip())
            if "TERAS" in description.upper():
                item_loader.add_value("terrace", True)
        
        if "DEPOZ" in description:
            deposit = description.split("DEPOZ")[1].split("TEK")[0].split("TL")[0]
            if "KİRA BEDELİ" not in deposit:
                item_loader.add_value("deposit",deposit.split(" ")[1])

        address=response.xpath("//ul[@class='overviewList']/li[contains(.,'Konum')]/span//text()").extract_first()
        if address:
            item_loader.add_value("address", address)
            if "fethiye" in address.lower() or "FETHİYE" in address.upper():
                city = "Fethiye"
                item_loader.add_value("city", city)
   
        coord=response.xpath("//div[@class='overview']/a[contains(@href,'maps')]/@href").get()
        if coord:
            item_loader.add_value("latitude", coord.split("/dir//")[-1].split(",")[0])
            item_loader.add_value("longitude", coord.split("/dir//")[-1].split(",")[1].split("/")[0])
        
        room=response.xpath("//table[@class='amentitiesTable']//tr[contains(.,'Oda Sayısı')]/td[2]/text()").extract_first()
        if room is not None:
                add=0
                room_array=room.split("+")
                for i in room_array:
                    add += int(i)
                item_loader.add_value("room_count",str(add) )
        else:
            if description.find("+")!=-1:
                
                index=description.index("+")
                find=description[index-1:index+2]
                if find:
                    add = int(find.split("+")[0])+int(find.split("+")[1])
                    item_loader.add_value("room_count",str(add) )

        bathroom_count=response.xpath("//table[@class='amentitiesTable']//tr[contains(.,'Banyo Sayısı')]/td[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        
        floor = response.xpath("//table[@class='amentitiesTable']//tr[contains(.,'Bulunduğu Kat')]/td[2]/text()").extract_first()
        if floor:          
            item_loader.add_value("floor", floor)

        furnished=response.xpath("//table[@class='amentitiesTable']//tr[contains(.,'Eşyalı')]/td[2]/text()").extract_first()
        if furnished:
            if "Evet" in furnished.strip():
                item_loader.add_value("furnished",True)
            if "Hayır" in furnished.strip():
                item_loader.add_value("furnished",False)
        
        balcony=response.xpath("//table[@class='amentitiesTable']//tr[contains(.,'Balkon')]/td[2]/text()").extract_first()
        if balcony:
            item_loader.add_value("balcony",True)
        
        swimming_pool=response.xpath("//table[@class='amentitiesTable']//tr[contains(.,'Havuz')]/td[2]/text()").extract_first()
        if swimming_pool:
            item_loader.add_value("swimming_pool",True)
           
        item_loader.add_xpath("landlord_name","//ul[@class='overviewList']//li[contains(.,'Yetkili: ')]/span/text()")
        item_loader.add_xpath("landlord_email","//ul[@class='overviewList']/li[contains(.,'Email')]/span/text()")
        item_loader.add_value("landlord_phone","+90 (252) 611 32 22")

       
        yield item_loader.load_item()