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

class MySpider(Spider):
    name = 'wengayrimenkul_com'
    execution_type='testing'
    country='turkey'
    locale='tr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.wengayrimenkul.com/ara/?post=emlakara&d=&l=&g=&tip=kiralik&anakategori=7&kategori=2&il=&minmetrekarem=&maxmetrekarem=&esyali=&otopark=&minfiyat=&maxfiyat=",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.wengayrimenkul.com/ara/?post=emlakara&d=&l=&g=&tip=kiralik&anakategori=7&kategori=4&il=&minmetrekarem=&maxmetrekarem=&esyali=&otopark=&minfiyat=&maxfiyat=&?d=&l=&g=&p=2&?d=&l=&g=&p=1",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.wengayrimenkul.com/ara/?post=emlakara&d=&l=&g=&tip=kiralik&anakategori=7&kategori=1&il=&minmetrekarem=&maxmetrekarem=&esyali=&otopark=&minfiyat=&maxfiyat=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.wengayrimenkul.com/ara/?post=emlakara&d=&l=&g=&tip=kiralik&anakategori=7&kategori=19&il=&minmetrekarem=&maxmetrekarem=&esyali=&otopark=&minfiyat=&maxfiyat=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.wengayrimenkul.com/ara/?post=emlakara&d=&l=&g=&tip=kiralik&anakategori=7&kategori=21&il=&minmetrekarem=&maxmetrekarem=&esyali=&otopark=&minfiyat=&maxfiyat=",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        for item in response.xpath("//div[@class='ilanBox']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        max_page = response.xpath("//ul[@class='pagination']/li[last()]//text()").get()
        if max_page:   
            if page <= int(max_page):
                url = response.url + f"&p={page}"
                yield Request(
                    url,
                    callback=self.parse,
                    meta={'property_type': response.meta.get('property_type'), "page": page+1}
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Wengayrimenkul_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//h1[@class='boxBaslik']/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title))
            if "TERAS" in title:
                item_loader.add_value("terrace",True)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        external_id=response.xpath("//table/tbody/tr/td[strong[.='İlan No']]//following-sibling::td/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(",")[0].strip()
            longitude = latitude_longitude.split('LatLng(')[1].split(",")[1].split(")")[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        address = ",".join(response.xpath("//div[@class='col-md-5']/div/a/text()").extract())
        if address:
            item_loader.add_value("address", address)
        item_loader.add_xpath("city", "normalize-space(//div[@class='col-md-5']/div/a)")
        
        square_meters=response.xpath("//table/tbody/tr/td[strong[.='Metre Kare (m²)']]//following-sibling::td/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())
        
        room_count  = response.xpath("normalize-space(//table/tbody/tr/td[strong[.='Oda Sayısı']]//following-sibling::td/text())").extract_first()
        if "Stüdyo" in room_count:
            item_loader.add_value("room_count","1")
        elif "+" in room_count:
            room_count=room_count.split("+")
            item_loader.add_value("room_count", str(int(room_count[0])+ int(room_count[1])))
        elif room_count:
            item_loader.add_value("room_count", room_count)
        elif response.xpath("normalize-space(//table/tbody/tr/td[strong[.='Bölüm & Oda Sayısı']]//following-sibling::td/text())"):
            item_loader.add_xpath("room_count", "normalize-space(//table/tbody/tr/td[strong[.='Bölüm & Oda Sayısı']]//following-sibling::td/text())")

        bathroom = response.xpath(
            "normalize-space(//table/tbody/tr/td[strong[.='Banyo Sayısı']]//following-sibling::td//text())"
            ).get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
     
        rent=response.xpath(
            "normalize-space(//div[@class='col-md-5']/div/span/text())"
            ).extract_first()
        if rent:
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "TRY")
        
        floor = response.xpath(
            "//table/tbody/tr/td[strong[.='Bulunduğu Kat']]//following-sibling::td/text()").extract_first()
        if floor:
            item_loader.add_value("floor", re.sub('\s{2,}', ' ', floor.strip()))
        
        utilities=response.xpath(
            "//table/tbody/tr/td[strong[contains(.,'Aidat')]]//following-sibling::td//text()"
            ).extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.strip())
            
        furnished=response.xpath(
            "//table/tbody/tr/td[strong[contains(.,'Eşyalı')]]//following-sibling::td/text()"
            ).extract_first()
        if furnished:
            if "Evet" in furnished:
                item_loader.add_value("furnished", True)
            if "Hayır" in furnished:
                item_loader.add_value("furnished", False)
        desc="".join(response.xpath("//div[contains(@class,'boxGenel p10')]/p//text() | //div[contains(@class,'boxGenel p10')]//ul/li//text() | //div[contains(@class,'boxGenel p10')]/span/text()").extract())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))

        images=[x for x in response.xpath("//div[@id='Carousel']/div/div/ul/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        balcony_=response.xpath("//div[@class='scrolldetay']/div[contains(., 'Balkon')]/text()").extract()
        balcony="".join(response.xpath("//div[contains(@class,'boxGenel p10')]/p[contains(.,'balkon')]//text()").extract())
        if balcony_:
            item_loader.add_value("balcony", True)
        elif balcony:
            item_loader.add_value("balcony", True)
        parking=response.xpath("//table/tbody/tr/td[strong[.='Otopark']]//following-sibling::td/text()").extract_first()
        if parking:
            if "Yok" in parking:
                item_loader.add_value("parking", False)  
            else:
                item_loader.add_value("parking", True)
        elif not parking:        
            parking="".join(response.xpath("//div[contains(@class,'boxGenel p10')]/p[contains(.,'otopark') or contains(.,'Otopark')]//text()").extract())
            park=response.xpath("//div[@class='scrolldetay']/div[contains(., 'Otopark') or contains(., 'Garaj')]/text()").extract()
            if park or parking:
                item_loader.add_value("parking", True)
        
        swimming=response.xpath("//div[@class='scrolldetay']/div[contains(., 'Yüzme')]/text()").extract()
        swimming_pool="".join(response.xpath("//div[contains(@class,'boxGenel p10')]/p[contains(.,'yüzme') or contains(.,'Yüzme')]//text()").extract())
        if swimming:
            item_loader.add_value("swimming_pool", True)
        elif swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        elevator=response.xpath("//div[@class='scrolldetay']/div[contains(., 'Asansör')]/text()").extract()
        if elevator:
            item_loader.add_value("elevator", True)
            
        washing_machine=response.xpath("//div[@class='scrolldetay']/div[contains(., 'Çamaşır Makinesi')]/text()").extract()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        dishwasher=response.xpath("//div[@class='scrolldetay']/div[contains(., 'Bulaşık Makinesi')]/text()").extract()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
            
        terrace=response.xpath("//div[@class='scrolldetay']/div[contains(., 'Teras')]/text()").extract()
        if terrace:
            item_loader.add_value("terrace", True)
         
             
        landlord_name=response.xpath("//div[contains(@class,'boxGenel p10')]/div[1]/a[1]/text()").extract_first()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name.strip())
        phone=response.xpath("//div[contains(@class,'boxGenel p10')]//span[i[contains(@class,'fa-phone')]]//text()").extract_first()
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        
        email=response.xpath("//div[contains(@class,'boxGenel p10')]//span[i[contains(@class,'fa-envelope')]]/text()").extract_first()
        if email:
            item_loader.add_value("landlord_email", email.strip())
            
        yield item_loader.load_item()