# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector 
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json
import dateparser
import re

class MySpider(Spider):
    name = 'bmvastgoed_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = "Bmvastgoed_PySpider_netherlands"
    post_urls = "https://www.bmvastgoed.nl/0-2ac6/aanbod-pagina"
                 
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en,tr-TR;q=0.9,tr;q=0.8,en-US;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36"
    }
    formdata = {
        "forsaleorrent": "FOR_RENT",
        "take": "10",
        "typegroups[0]": "19",
    }
    def start_requests(self):
        yield FormRequest(self.post_urls,
                    callback=self.parse,
                    formdata=self.formdata,
                    headers = self.headers
        ) 

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 10)
        seen=False

        for item in response.xpath("//article[@class='col-xs-12 col-md-6 objectcontainer']"):
            if item.xpath(".//span[contains(.,'Verhuurd')]"):
                continue
            
            base_url = "https://www.bmvastgoed.nl"
            f_url = item.xpath(".//div[@class='datacontainer']/a/@href").get()
            follow_url = base_url + f_url
            yield Request(follow_url, callback=self.populate_item)
            seen = True

        if page == 10 or seen:
            formdata2 = {
                "forsaleorrent": "FOR_RENT",
                "take": "10",
                "typegroups[0]": "19",
                "skip": str(page),
            } 

            url = "https://www.bmvastgoed.nl/0-2ac6/aanbod-pagina"

            yield FormRequest(url,
                        callback=self.parse,
                        formdata=formdata2,
                        headers = self.headers,
                        meta={"page": page+10}
            ) 

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        prop_type = response.xpath("(//td[@class='object_detail_title'][contains(.,'Type')])[1]/following-sibling::td/text()").get()
        if prop_type and "apartment" in prop_type:
            item_loader.add_value("property_type", "apartment")
        
        title = response.xpath("//h1[@class='object_title']/text()").get()
        if title:
            item_loader.add_value("title", title)

        rent = response.xpath("//div[@class='object_price']/text()").get()
        if rent:
            rent = rent.split(',')[0].split('€')[-1].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'EUR')

        room_count = response.xpath("(//td[@class='object_detail_title'][contains(.,'Aantal kamer')])[1]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(' ')[0])

        square_meters = response.xpath("(//td[@class='object_detail_title'][contains(.,'Gebruiksoppervlakte')])[1]/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(' ')[0].split(',')[0].strip())   
        
        deposit = response.xpath("(//td[@class='object_detail_title'][contains(.,'Borg')])[1]/following-sibling::td/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(',')[0].split('€')[-1].strip())   

        external_id = response.url.split('ref-')[-1].split('?')[0].strip()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("(//td[@class='object_detail_title'][contains(.,'Adres')])[1]/following-sibling::td/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.split(', ')[1].split(' ')[0]
            item_loader.add_value("zipcode", zipcode)
        
        terrace = response.xpath("//td[@class='object_detail_title']/following-sibling::td/text()[contains(.,'terras')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        description = " ".join(response.xpath("//div[@class='description textblock']/div/text()").getall()).strip()   
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='object-photos']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        try:
            latitude = response.xpath("//script[@type='text/javascript']/text()[contains(.,'center')]").get()
            if latitude:
                item_loader.add_value("latitude", latitude.split('center: [')[1].split(',')[1].split(']')[0].strip())
                item_loader.add_value("longitude", latitude.split('center: [')[1].split(',')[0].strip())           
        except:
            pass
        
        landlord_name = response.xpath("//div[@class='object_detail_contact_name']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "BM Vastgoed")
        landlord_email = response.xpath("//a[@class='object_detail_contact_email obfuscated-mail-link']/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        else:
            item_loader.add_value("landlord_email", "contact@bmvastgoed.nl")
        
        item_loader.add_value("landlord_phone", "085-7325910")
        
        yield item_loader.load_item()
