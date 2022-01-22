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
import scrapy, copy, urllib

class MySpider(Spider):
    name = 'chiusano_com'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Chiusano_PySpider_italy" 

    def start_requests(self): 
        start_urls = [
            {
                "url" : [
                    "https://www.chiusano.com/r/annunci/affitto-.html?Codice=&Motivazione%5B%5D=2&Tipologia%5B%5D=1&Tipologia%5B%5D=37&Tipologia%5B%5D=38&Tipologia%5B%5D=8&search_comune=&via_indirizzo=&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&Piano%5B%5D=0&cf=yes",
                ],      
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.chiusano.com/r/annunci/affitto-.html?Codice=&Motivazione%5B%5D=2&Tipologia%5B%5D=36&Tipologia%5B%5D=140&Tipologia%5B%5D=325&Tipologia%5B%5D=9&Tipologia%5B%5D=43&Tipologia%5B%5D=51&Tipologia%5B%5D=327&Tipologia%5B%5D=48&Tipologia%5B%5D=49&search_comune=&via_indirizzo=&Prezzo_da=&Prezzo_a=&Totale_mq_da=&Totale_mq_a=&Piano%5B%5D=0&cf=yes",
                ],
                "property_type" : "house" 
            } 
          
        ] 
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response,**kwargs):
        page = response.meta.get("page", 1)
        seen = False
        for url in response.xpath("//ul/li/section//a/@href").getall():
            if not "emailalert" in url:
                yield Request(url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
                seen = True
        
 
        if page == 1 or seen: 

            p_url = f"https://www.chiusano.com/moduli/realestate/immobili_elenco_dettaglio.php?p={page}&loadAjax=yes"
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Mobile Safari/537.36"
            }
            yield Request(
                p_url,
                callback=self.parse,headers=headers,
                meta={'property_type': response.meta['property_type'], "page":page+1}
            ) 

    def populate_item(self, response,**kwargs):
        item_loader = ListingLoader(response=response)

        check_page = response.url
        if check_page and "404.php" in check_page:
            return
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        
        external_id=response.url
        if external_id:
            external_id=external_id.split("--")[0].split("i/")[-1]
            if external_id:
                item_loader.add_value("external_id",external_id)
        address=" ".join(response.xpath("//div[@class='bottom_tit1']//div//text()").getall())
        if address:
            item_loader.add_value("address",address)
        city=response.xpath("//div[@class='bottom_tit1']//div[@class='titB']//text()").get()
        if city:
            item_loader.add_value("city",city)
        zipcode=response.xpath("//title/text()").get()
        if zipcode:
            zipcode=zipcode.split(".")[-1].strip()
            if zipcode:
                item_loader.add_value("zipcode",zipcode)

        rent=response.xpath("//div[@class='titolo2']//div[@class='titB']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].strip())
        item_loader.add_value("currency", "EUR")
        
        utilities=response.xpath("//strong[contains(.,'Spese')]/parent::div/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split('€')[-1].strip().split(' ')[0])

        square_meters=response.xpath("//strong[contains(.,'mq')]/parent::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("mq")[0].strip())
        
        desc=" ".join(response.xpath("//div[@class='decrizioneSch']/p//text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        room=response.xpath("//div[@class='dettagli']//div[@class='ico-35-camere']/span/text()").get()
        if room:
            item_loader.add_value("room_count",room)
        bath=response.xpath("//div[@class='dettagli']//div[@class='ico-35-bagni']/span/text()").get()
        if bath:
            item_loader.add_value("bathroom_count",bath)
        terrace=response.xpath("//div[@class='dettagli']//div[@class='ico-35-terrazzo']").get()
        if terrace:
            item_loader.add_value("terrace",True)
        parking=response.xpath("(//div[@class='ico-35-boxauto'])[1]").get()
        if parking:
            item_loader.add_value("parking",True)
        floor=response.xpath("//div[@class='dettagli']//div[@class='ico-35-piano']/span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        images=response.xpath("//div[@class='cont_img_small ']//a/@href").getall()
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Chiusano & C")
        phone=response.xpath("//a[contains(@href,'tel:')]/@href").get()
        if phone:
            item_loader.add_value("landlord_phone",phone.split(":")[-1])

        position = response.xpath("//script[contains(text(),'var lat')]/text()").get()
        if position:
            lat = re.search('lat = "([\d.]+)";', position).group(1)
            long = re.search('lgt = "([\d.]+)";', position).group(1)
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)

        landlord_email = response.xpath("//span[@class='call_email']/a/@href").get()
        if landlord_email:
            landlord_email = landlord_email.split("to:")[-1].strip(";")
            item_loader.add_value("landlord_email",landlord_email)

        yield item_loader.load_item()