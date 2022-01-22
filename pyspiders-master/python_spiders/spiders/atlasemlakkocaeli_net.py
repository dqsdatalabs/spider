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
import math
import re

class MySpider(Spider):
    name = 'atlasemlakkocaeli_net'

    execution_type='testing'
    country='turkey'
    locale='tr'
    external_source='Atlasemlakkocaeli_PySpider_turkey_tr'
    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.atlasemlakkocaeli.net/kiralik/konut/residence?git=1",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.atlasemlakkocaeli.net/kiralik/konut/villa?git=1",
                "property_type" : "house"
            },
            {
                "url" : "http://www.atlasemlakkocaeli.net/kiralik/konut/bina?git=1",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.atlasemlakkocaeli.net/kiralik/konut/apartman-dairesi?git=1",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//tr/td[1]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        
        next_page = response.xpath("//a[.='Sonraki']/@href").get()
        if next_page and next_page != response.url:
            yield Request(
                url=next_page,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        room=response.xpath("//div[@class='ilanozellikler']//tr[contains(.,'Oda Sayısı')]/td[2]/text()").extract_first()

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = response.xpath("//title//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        item_loader.add_xpath("external_id", "//div[@class='ilanozellikler']//tr[contains(.,'İlan No')]/td[2]/text()")       
        item_loader.add_value("external_source", "Atlasemlakkocaeli_PySpider_"+ self.country + "_" + self.locale)
        
        address=" ".join(response.xpath("//div[@class='ilanozellikler']//tr[2]//a/text()").extract())
        if address:
            item_loader.add_value("address",address)

        item_loader.add_xpath("city","//div[@class='ilanozellikler']//tr[2]//a[1]/text()")

        description=" ".join(response.xpath("//div[@class='ilanaciklamalar'][contains(.,'Açıklama')]/*[not(self::h3)]//text()").extract())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description",description)
        
        item_loader.add_xpath("square_meters", "//div[@class='ilanozellikler']//tr[contains(.,'M² ')]/td[2]/text()[.!='0']")
        
        item_loader.add_xpath("floor", "//div[@class='ilanozellikler']//tr[contains(.,'Bulunduğu Kat')]/td[2]/text()")
                            
        item_loader.add_value("currency", "TRY")
        rent=response.xpath("//div[@class='ilanozellikler']//tr[1]//text()[normalize-space()]").extract_first()
        if rent is not None:
                item_loader.add_value("rent",rent.strip("TL"))
        utilities=response.xpath("//div[@class='ilanozellikler']//tr[contains(.,'Aidat')]/td[2]/text()").extract_first()
        if utilities is not None:
                item_loader.add_value("utilities",utilities.strip("TL"))
        if room is not None:
            add=0
            room_array=room.split("+")
            for i in room_array:
                add += int(math.floor(float(i)))
            item_loader.add_value("room_count",str(add) )
        
        bathroom=response.xpath("//div[@class='ilanozellikler']//tr[contains(.,'Banyo')]/td[2]/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        
        
        furnished=response.xpath("//div[@class='ilanozellikler']//tr[contains(.,'Eşyalı mı')]/td[2]/text()").extract_first()
        if furnished:
            if "evet" in furnished.lower():
                item_loader.add_value("furnished",True)
            elif "hayır" in furnished.lower():
                item_loader.add_value("furnished",False)
        elif not furnished:
            furnished=response.xpath("//div[@class='ilanozellik']//span[@id='ozellikaktif'][contains(.,'Mobilya')]").extract_first()
            if furnished is not None:
                item_loader.add_value("furnished",True)
            else:
                item_loader.add_value("furnished",False)


        elevator=response.xpath("//div[@class='ilanozellik']//span[@id='ozellikaktif'][contains(.,'Asansör')]").extract_first()
        if elevator is not None:
            item_loader.add_value("elevator",True)
        else:
            item_loader.add_value("elevator",False)

        parking=response.xpath("//div[@class='ilanozellik']//span[@id='ozellikaktif'][contains(.,'Otopark')]").extract_first()
        if parking is not None:
                item_loader.add_value("parking",True)
        else:
            item_loader.add_value("parking",False)

        swimming_pool=response.xpath("//div[@class='ilanozellik']//span[@id='ozellikaktif'][contains(.,'Yüzme Havuzu')]").extract_first()
        if swimming_pool is not None:
                item_loader.add_value("swimming_pool",True)
        else:
            item_loader.add_value("swimming_pool",False)
    
        terrace=response.xpath("//div[@class='ilanozellik']//span[@id='ozellikaktif'][contains(.,'Teras')]").extract_first()
        if terrace is not None:
            item_loader.add_value("terrace",True)
        else:
            item_loader.add_value("terrace",False)
        
        balcony=response.xpath("//div[@class='ilanozellik']//span[@id='ozellikaktif'][contains(.,'Balkon')]").extract_first()
        if balcony is not None :
            item_loader.add_value("balcony",True)
        else:
            item_loader.add_value("balcony",False)

        machine=response.xpath("//div[@class='ilanozellik']//span[@id='ozellikaktif'][contains(.,'Beyaz Eşya')]").extract_first()
        

        dishwasher=response.xpath("//div[@class='ilanozellik']//span[@id='ozellikaktif'][contains(.,'Bulaşık Makinesi')]").extract_first()
        all_machines=response.xpath("//div[@class='ilanozellik']//span[@id='ozellikaktif'][contains(.,'Beyaz Eşya')]").extract_first()
        if dishwasher is not None or all_machines:
                item_loader.add_value("dishwasher",True)
        else:
            if machine is not None:
                item_loader.add_value("dishwasher",True)
            else:
                item_loader.add_value("dishwasher",False)

        washing_machine=response.xpath("//div[@class='ilanozellik']//span[@id='ozellikaktif'][contains(.,'Çamaşır Makinesi')]").extract_first()
        if washing_machine is not None or all_machines :
                item_loader.add_value("washing_machine",True)
        else:
            if machine is not None:
                item_loader.add_value("washing_machine",True)
            else:
                item_loader.add_value("washing_machine",False)
    
        images = [response.urljoin(x) for x in response.xpath("//ul[contains(@class,'gallery')]//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)      

        item_loader.add_xpath("latitude","//input[@id='g_lat']/@value")
        item_loader.add_xpath("longitude","//input[@id='g_lng']/@value")

        landlord_name=response.xpath("//div[@class='danisman']/h4//text()").extract_first()
        if landlord_name is not None:
            if "/" in landlord_name:
                item_loader.add_value("landlord_name",landlord_name.split("/")[0])
            else: 
                item_loader.add_value("landlord_name",landlord_name)
        item_loader.add_xpath("landlord_email","//div[@class='danisman']/h5[contains(.,'E-Posta')]/text()")
        item_loader.add_xpath("landlord_phone","//div[@class='danisman']/h5[contains(.,'Telefon')]/a/text()")

        yield item_loader.load_item()