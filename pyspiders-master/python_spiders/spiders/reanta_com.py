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
    name = 'reanta_com'
    execution_type='testing' 
    country='turkey'
    locale='en'    
    def start_requests(self):
        start_urls = [
            {
                "url" : ["http://reanta.com/house-villa-rent-in-antalya"],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://reanta.com/duplex-penthouse-rent-antalya",
                    "http://reanta.com/apartments-for-rent-antalya/4-br-apartments",
                    "http://reanta.com/apartments-for-rent-antalya/3-br-apartments",
                    "http://reanta.com/apartments-for-rent-antalya/2-br-apartments",
                    "http://reanta.com/apartments-for-rent-antalya/1-br-apartments",
                ],
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//dt[@class='title']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
    
        next_page = response.xpath("//a[@title='Next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Reanta_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//div[@id='component']/h1/text()").get()
        item_loader.add_value("title", title)
        for i in city_list:
            if i.lower() in title.lower():
                item_loader.add_value("city", i)
                break
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        external_id=response.xpath("//div[@id='component']/h2/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[1].strip())
        
        rent="".join(response.xpath(
            "//div[@id='component']/table/tr[contains(.,'Rent')]//following-sibling::td//text()"
            ).getall()).strip()
        if rent=='Consult us':
            pass
        elif "day" in rent:
            price=rent.split("EUR")[0].strip()
            item_loader.add_value("rent", str(int(price)*30))
        elif rent:
            item_loader.add_value("rent", rent.strip().split(' ')[0])
        
        item_loader.add_value("currency", "EUR")
        
        square_meters=response.xpath("//div[@id='component']/table/tr[contains(.,'Living space')]//following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].strip())
        
        room_count1=response.xpath("//div[@id='component']/table/tr[contains(.,'Number of bedroom')]//following-sibling::td/text()").get()
        room_count2=response.xpath("//div[@id='component']/table/tr[contains(.,'Number of room')]//following-sibling::td/text()").get()
        item_loader.add_value("room_count", int(room_count1)+int(room_count2))
        
        bathroom_count=response.xpath("//div[@id='component']/table//tr[contains(.,'bathroom')]//following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        address=response.xpath("//div[@id='component']/div/h3[contains(.,'Address')]//following-sibling::address/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        desc="".join(response.xpath("//div[contains(@class,'property-description clr')]/*[self::p or self::table]//text()").getall())
        if desc:
            item_loader.add_value("description",re.sub('\s{2,}', ' ', desc) )
        
        if "washing machine" in desc.lower():
            item_loader.add_value("washing_machine", True)
        
        swimming_pool=response.xpath("//div[@id='component']/div/ul/li[contains(.,'Swimming')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        floor=response.xpath("//div[@id='component']/table/tr[contains(.,'Floor')][1]//following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
            
        furnished=response.xpath("//div[@id='component']/table/tr[contains(.,'Property condition')][1]//following-sibling::td/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        furnishedcheck=item_loader.get_output_value("furnished")
        if not furnishedcheck:
            furnished=response.xpath("//div[@class='property-description clr']/p/strong/text()").get()
            if furnished and "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)
        parking=response.xpath("//div[@class='property-description clr']//table/tbody/tr[2]/td[2]/p//img//@title").getall()
        if parking:
            for i in parking:
                if "parking" in i.lower():
                    item_loader.add_value("parking",True)

            
        images=[x for x in response.xpath("//div[@id='jea-gallery-scroll']/a/img/@src").getall()]
        for image in images:
            item_loader.add_value("images","http://reanta.com"+image)
        item_loader.add_value("external_images_count", str(len(images)))
        
        deposit=response.xpath("//div[@id='component']/table/tr[contains(.,'Deposit')]//following-sibling::td//text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(" ")[0])
        
        item_loader.add_value("landlord_name", "REANTA")
        tel=response.xpath("//meta[@name='description']/@content").get()
        if tel:
            phone=tel.split("Tel")[-1].split(",")[0].replace(".","")
            item_loader.add_value("landlord_phone",phone)
        email=response.xpath("//meta[@name='description']/@content").get()
        if email:
            email=email.split("e-mail")[-1].replace(":","")
            item_loader.add_value("landlord_email",email)
        
        yield item_loader.load_item()

city_list=["Adana", "Adıyaman", "Afyon", "Ağrı", "Amasya", "Ankara", "Antalya", "Artvin", "Aydın", "Balıkesir", "Bilecik", "Bingöl", "Bitlis", "Bolu", "Burdur", "Bursa", "Çanakkale", "Çankırı", "Çorum", "Denizli", "Diyarbakır", "Edirne", "Elazığ", "Erzincan", "Erzurum", "Eskişehir", "Gaziantep", "Giresun", "Gümüşhane", "Hakkari", "Hatay", "Isparta", "İçel (Mersin)", "İstanbul", "İzmir", "Kars", "Kastamonu", "Kayseri", "Kırklareli", "Kırşehir", "Kocaeli", "Konya", "Kütahya", "Malatya", "Manisa", "Kahramanmaraş", "Mardin", "Muğla", "Muş", "Nevşehir", "Niğde", "Ordu", "Rize", "Sakarya", "Samsun", "Siirt", "Sinop", "Sivas", "Tekirdağ", "Tokat", "Trabzon", "Tunceli", "Şanlıurfa", "Uşak", "Van", "Yozgat", "Zonguldak", "Aksaray", "Bayburt", "Karaman", "Kırıkkale", "Batman", "Şırnak", "Bartın", "Ardahan", "Iğdır", "Yalova", "Karabük", "Kilis", "Osmaniye", "Düzce"]


