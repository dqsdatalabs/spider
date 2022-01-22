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
    name = 'izmirpointgayrimenkul_com'
    headers = {
        "content-type": "text/html; charset=utf-8",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
    }

    execution_type = 'testing'
    country = 'turkey'
    locale = 'tr' # LEVEL 1


    def start_requests(self):
        start_urls = [
            {
                "url" : "https://izmirpointgayrimenkul.com/ilanlar.aspx?gid=1&ktipid=2&kulalanid=1&temid=0&danid=0&sehirid=&ilceid=&mahid=&fiyat1=&fiyat2=&ilantip=%27i%27",
                "property_type" : "apartment"
            },
            {
                "url" : "https://izmirpointgayrimenkul.com/ilanlar.aspx?gid=1&ktipid=2&kulalanid=2&temid=0&danid=0&sehirid=&ilceid=&mahid=&fiyat1=&fiyat2=&ilantip=%27i%27",
                "property_type" : "apartment"
            },
            {
                "url" : "https://izmirpointgayrimenkul.com/ilanlar.aspx?gid=1&ktipid=2&kulalanid=4&temid=0&danid=0&sehirid=&ilceid=&mahid=&fiyat1=&fiyat2=&ilantip=%27i%27",
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response):
        for item in response.xpath("//div[@class='property-box']/div[@class='detail']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, headers= self.headers, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rented = response.xpath("//img[@id='RP_ilanlar_ctl01_Image1']/@src").extract_first()
        if rented:
            return
        item_loader.add_value("external_source", "Izmirpointgayrimenkul_PySpider_" + self.country + "_" + self.locale)
        price = response.xpath("//span[@id='LBL_Fiyat']/text()").extract_first()
        room_count = "".join(response.xpath("//tr[td[. ='Oda Sayısı']]/td/text()").extract())
        
        title=response.xpath("//span[@id='LBL_Baslik']/text()").get()
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath("external_id", "//tr[td[. ='İlan No']]/td/span/text()")
        item_loader.add_xpath("square_meters", "//tr[td[. ='m²']]/td/span/text()")

        item_loader.add_value("property_type", response.meta.get('property_type'))

        if price:
            if price=='0': return
            price = price.replace(".","")
            item_loader.add_value("rent", price)
            
        currency = "".join(response.xpath("//span[@id='LBL_ParaBirimi']/text()").extract())
        if currency:
            item_loader.add_value("currency",currency.replace('TL','TRY'))
        else:
            item_loader.add_value("currency", "EUR")

        if room_count:
            if "Stüdyo" in room_count:
                item_loader.add_value("room_count", "1")
            elif "(" in  room_count:
                item_loader.add_value("room_count", room_count.strip().split("(")[1].split(")")[0])
            elif "+" in  room_count:
                item_loader.add_value("room_count", split_room(room_count, "count"))
            else:
                item_loader.add_value("room_count", room_count)

        bathroom_count=response.xpath("//tr[td[. ='Banyo Sayısı']]/td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        floor = "".join(response.xpath("//tr[td[. ='Bulunduğu Kat']]/td/text()[not(contains(.,'Villa Tipi') or contains(.,'Müstakil'))]").extract())
        if floor:
            item_loader.add_value("floor", floor.strip())

        desc = "".join(response.xpath("//span[@id='LBL_Aciklama']//text()").extract())
        item_loader.add_value("description", desc.strip())
        
        furnished = response.xpath(
            "//span[@id='LBL_Aciklama']//text()[contains(.,'eşyalı') or contains(.,'EŞYALI')]"
            ).get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        
        address = response.xpath("//span[@id='LBL_Lokasyon']/text()").extract_first()
        if address:
            item_loader.add_value("address", address.replace("/"," "))

        item_loader.add_value("city", address.split("/")[0])

        images = [response.urljoin(x)for x in response.xpath("//div[contains(@class,'carousel')]/ul/li/a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        terrace = "".join(response.xpath("//div[contains(@class,'tab-pane')]//h3[contains(.,'Dış Özellikler')]//following-sibling::div/div/ul/li[@class='yes'][contains(.,'Yüzme Havuzu')]").extract())
        if terrace:
            item_loader.add_value("swimming_pool", True)

        terrace = "".join(response.xpath("//div[contains(@class,'tab-pane')]//h3[contains(.,'Dış Özellikler')]//following-sibling::div/div/ul/li[@class='yes'][contains(.,'Otopark')]").extract())
        if terrace:
            item_loader.add_value("parking", True)

        terrace = "".join(response.xpath("//div[contains(@class,'tab-pane')]//h3[contains(.,'Dış Özellikler')]//following-sibling::div/div/ul/li[@class='yes'][contains(.,'Asansör')]").extract())
        if terrace:
            item_loader.add_value("elevator", True)


        terrace = "".join(response.xpath("//div[contains(@class,'tab-pane')]//h3[contains(.,'Dış Özellikler')]//following-sibling::div/div/ul/li[@class='yes'][contains(.,'Balkon')]").extract())
        if terrace:
            item_loader.add_value("balcony", True)

        dishwasher=response.xpath("//span[@id='LBL_Aciklama']//text()[contains(.,'Bulaşık') or contains(.,'bulaşık')]").getall()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing=response.xpath("//span[@id='LBL_Aciklama']//text()[contains(.,'Çamaşır') or contains(.,'çamaşır')]").getall()
        if washing:
            item_loader.add_value("washing_machine", True)

        if "eşyalı" in title.lower():
            item_loader.add_value("furnished", True)
        
        if "teras" in desc.lower():
            item_loader.add_value("terrace", True)
        
        item_loader.add_xpath("latitude", "substring-before(substring-after(//script/text()[contains(.,'var latlng')],'google.maps.LatLng('),',')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(substring-after(//script/text()[contains(.,'var latlng')],'google.maps.LatLng('),', '),')')")

        item_loader.add_xpath("landlord_phone", "normalize-space(//ul/li[span[.='Telefon:']]/a/text())")
        item_loader.add_xpath("landlord_email", "//ul/li[span[.='E-Posta:']]/a/@href")
        item_loader.add_xpath("landlord_name", "//div[@class='details']/h3//text()")

        yield item_loader.load_item()

def split_room(room_count,get):
    count1 = room_count.strip().split("+")[0]
    count2 = room_count.strip().split("+")[1]
    count = int(count1)+int(count2)
    return str(count)


    

    
