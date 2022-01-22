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
    name = 'realiteyatirim_com'
    execution_type='testing'
    country='turkey'
    locale='tr' 
    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.realiteyatirim.com/kiralik/dublex-daire/ilanlari/075105114097108196177107/068117098108101120032068097105114101/65/1",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.realiteyatirim.com/kiralik/esyali-daire/ilanlari/075105114097108196177107/069197159121097108196177032068097105114101/62/1",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.realiteyatirim.com/kiralik/daire/ilanlari/075105114097108196177107/068097105114101/4/1",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//p[@class='ilanaramabaslik']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        
        next_page = response.xpath("//a[.='»']/@href").get()
        if next_page:
            yield Request(
                url=next_page,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Realiteyatirim_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//h1[contains(@class,'detaybaslik')]//text()")
        

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        external_id=response.xpath("//div[@id='pinBoxContainer']/div/ol/li/em/span//text()").extract_first()
        item_loader.add_value("external_id", external_id.split(":")[1].strip())
        rent=response.xpath("//div[@class='col-sm-5']/div/b/text()").extract_first()
        if rent:
            item_loader.add_value("rent", rent.strip())
            item_loader.add_value("currency", "TRY")
        address=response.xpath("//div[@id='pinBoxContainer']/div/ol/li/a/span/text()").extract()
        if address:
            item_loader.add_value("city", str(address[-3]))
            item_loader.add_value("address", str(address[-1])+" "+str(address[-2])+" "+str(address[-3]))
        
        latitude=response.xpath("//meta/@itemprop[contains(.,'latitude')]/parent::meta/@content").get()
        longitude=response.xpath("//meta/@itemprop[contains(.,'longitude')]/parent::meta/@content").get()
        if latitude and longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        square_meters=response.xpath("//div[@class='col-sm-5']/table/tbody/tr[contains(., 'Net Metrekare')]//following-sibling::td/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
        room_count=response.xpath("//h1[contains(@class,'detaybaslik')]//text()").extract_first().split(" ")
        for i in room_count:
            if "+" in i:
                i=i.split("+")
                item_loader.add_value("room_count", str(int(i[0])+int(i[1])))
                break
        
        bathroom_count=response.xpath("//td[contains(.,'Banyo')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor=response.xpath("//div[@class='col-sm-5']/table/tbody/tr[contains(., 'Bulunduğu')]//following-sibling::td/text()[not(contains(.,'Giriş')) and not(contains(.,'Bahçe Katı'))]").extract_first()
        if floor:
            item_loader.add_value("floor", floor.split(".")[0])
        
        balcony=response.xpath("//div[@class='col-sm-5']/table/tbody/tr[contains(., 'Balkon')]//following-sibling::td[contains(., 'Evet')]/text()").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
            
        terrace=response.xpath("//div[@class='col-sm-5']/table/tbody/tr[contains(., 'Teras')]//following-sibling::td[contains(., 'Evet')]/text()").extract_first()
        if terrace:
            item_loader.add_value("terrace", True)
        
        utilities=response.xpath("//div[@class='col-sm-5']/table/tbody/tr[contains(., 'Aidat')]//following-sibling::td/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities)  
             
        elevator=response.xpath("//strong[@class='secili'][contains(., 'Asansör')]/text()").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)
            
        dishwasher=response.xpath("//strong[@class='secili'][contains(., 'Bulaşık Makinesi')]/text()").extract_first()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        washing=response.xpath("//strong[@class='secili'][contains(., 'Çamaşır Makinesi')]/text()").extract_first()
        if washing:
            item_loader.add_value("washing_machine", True)    
        furnished=response.xpath("//strong[@class='secili'][contains(., 'Mobilyalı')]/text()").extract_first()
        if furnished:
            item_loader.add_value("furnished", True)
                
        images=[x for x in response.xpath("//div[@class='demo-gallery']//ul/li/a/img/@src").getall()]   
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        desc="".join(response.xpath("//div[@class='col-sm-12']/span/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        parking=response.xpath("//strong[@class='secili'][contains(., 'Otopark')]/text()").extract()
        if parking:
            item_loader.add_value("parking", True)
        elif "Otopark" in desc:
            item_loader.add_value("parking", True)
        swimming_pool=response.xpath("//strong[@class='secili'][contains(., 'Yüzme Havuzu')]/text()").extract()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        landlord_name=response.xpath("//div[@class='media-body']/p[1]/text()").extract_first()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        landlord_phone=response.xpath("//div[@class='media-body']/p/span[@itemprop='telephone']/text()").extract_first()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        landlord_email=response.xpath("//div[@class='media-body']/p[contains(.,'@')]//text()").extract_first()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.strip())
        
        
        
            
        yield item_loader.load_item()