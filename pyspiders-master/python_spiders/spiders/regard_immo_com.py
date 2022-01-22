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
    name = 'regard_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    headers = {
        'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
        'cache-control': "no-cache",
    }
    def start_requests(self):

        start_urls = [
            {
                "payload" : "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"TypeTransac\"\r\n\r\nL\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"Villes\"\r\n\r\nSecteurs\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"typebien\"\r\n\r\n1|un|appartement\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"SurfMini\"\r\n\r\nSurface minimum\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"PiecesMini\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"PiecesMaxi\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"ChambresMini\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"ChambresMaxi\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"PrixMini\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"PrixMaxi\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"Motcle\"\r\n\r\nContenant le mot...\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"secteurs\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--",
                "property_type" : "apartment"
            },
            {
                "payload" : "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"TypeTransac\"\r\n\r\nL\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"Villes\"\r\n\r\nSecteurs\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"typebien\"\r\n\r\n6|une|maison\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"SurfMini\"\r\n\r\nSurface minimum\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"PiecesMini\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"PiecesMaxi\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"ChambresMini\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"ChambresMaxi\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"PrixMini\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"PrixMaxi\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"Motcle\"\r\n\r\nContenant le mot...\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"secteurs\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--",
                "property_type" : "house"
            },
            
        ] #LEVEL-1

        for url in start_urls:        
            yield Request(url="https://www.regard-immo.com/search.asp?x=",
                                 callback=self.parse,
                                 method="POST",
                                 body=url.get("payload"),
                                 headers=self.headers,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//img[@class='img_inner']/../@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )

        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        print(response.url)
        item_loader.add_value("external_source", "Regard_immo_PySpider_"+ self.country + "_" + self.locale)
        address = response.xpath("//div[@class='pad1']/h1/text()").extract_first()
        if address:
            item_loader.add_value("address",address.split("- ")[1].strip())

        title="".join(response.xpath("//h1/text()").extract())
        if title:
            item_loader.add_value("title",title)

        price = response.xpath("//div[contains(@class,'titbig')]/text()[.!='Prix: NOUS CONSULTER']").extract_first()
        if price:
            price = price.split(":")[1]
            item_loader.add_value("rent_string", price.replace("\xa0","."))
        
        item_loader.add_xpath("external_id", "//div[@class='uls']//li[contains(.,'Réf.')]/b/text()")
       
        room_count = response.xpath("//div[@class='uls']//li[contains(.,'pièce')]/b/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split("pièce")[0].strip() )
       
        square = response.xpath("//div[@class='uls']//li[contains(.,'Surface')]/b/text()").extract_first()
        if square:
            square_meters =square.split("m")[0].strip()
            item_loader.add_value("square_meters",square_meters )
        
        desc = "".join(response.xpath("//p[@class='overwrap']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "balcon" in desc:
                item_loader.add_value("balcony", True)
            if "garage" in desc:
                item_loader.add_value("parking", True)

        energy = response.xpath("//td[contains(@class,'colorDPED')]//text()").extract_first()
        if energy:
            item_loader.add_value("energy_label", energy)
        utilities = response.xpath("//tr/td[contains(.,'Charges')]/following-sibling::td/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0])
          
        images = [response.urljoin(x) for x in response.xpath("//ul[@id='image-gallery']/li/img/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "06 36 57 40 17")
        item_loader.add_value("landlord_name", "REGARD IMMOBILIER")
        if "Vente" in title:
            pass
        else:
            yield item_loader.load_item()
