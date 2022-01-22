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
    name = 'marmaraemlak_com'
    execution_type='testing'
    country='turkey'
    locale='tr'
    thousand_seperator=','
    scale_seperator='.'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.emlakjet.com/emlak-ofisleri-detay/marmara-emlak-32992/?gm_tipi=2&gm_durumu=2", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'styles_gridColumn')]/div"):
            href = item.xpath("./div/a/@href").get()
            address = item.xpath("./a/div/p[contains(@class,'styles_listingDetailAdress')]//text()").get()
            follow_url = response.urljoin(href)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), 'address': address} )
            seen = True
        
        if page == 1 or seen:
            url = f"https://www.emlakjet.com/emlak-ofisleri-detay/marmara-emlak-32992/{page}/?gm_tipi=2&gm_durumu=2"
            yield Request(url, callback=self.parse, meta={"page": page+1,'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Marmaraemlak_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//div/h1/text()")
        
        rent=response.xpath("//div[contains(@class,'styles_price__')]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(".","").replace(",","").replace(" ",""))  
            item_loader.add_value("currency", "TRY")

        
        square_mt=response.xpath("//div[contains(@class,'styles_tableColumn')][contains(.,'Net')]//following-sibling::div//text()").get()
        if square_mt:
            item_loader.add_value("square_meters", square_mt.split(' ')[0])

        
        room_count=response.xpath("//div[contains(@class,'styles_tableColumn')][contains(.,'Oda Sayısı')]//following-sibling::div//text()").get()
        if room_count=='Stüdyo':
            item_loader.add_value("room_count", "1")
        elif "+" in room_count:
            room_count=room_count.split('+')
            item_loader.add_value("room_count", str(int(room_count[0])+int(room_count[1])))
        elif room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = "".join(response.xpath("//div[contains(@class,'styles_tableColumn')][contains(.,'Banyo')]//following-sibling::div//text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        address = response.meta.get('address')
        if address:
            item_loader.add_value("address", address)
        
        
        external_id=response.xpath("//div[contains(@class,'styles_tableColumn')][contains(.,'İlan Numarası')]//following-sibling::div//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        floor=response.xpath(
            "//div[contains(@class,'styles_tableColumn')][contains(.,'Bulunduğu')]//following-sibling::div//text()"
            ).get()
        if "." in floor:
            item_loader.add_value("floor", floor.split('.')[0])
        
        utilities=response.xpath("//div[contains(@class,'styles_tableColumn')][contains(.,'Aidat')]//following-sibling::div//text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('TL')[0].strip())
        
        deposit=response.xpath("//div[contains(@class,'styles_tableColumn')][contains(.,'Depozito')]//following-sibling::div//text()").get()
        if deposit:
            deposit = deposit.replace(",","")
            item_loader.add_value("deposit", deposit.split('TL')[0].strip())
            
             
        furnished=response.xpath("//div[contains(@class,'styles_tableColumn')][contains(.,'Eşya')]//following-sibling::div//text()").get()
        if furnished!='Boş':
            item_loader.add_value("furnished", True )
        furnishedcheck=item_loader.get_output_value("furnished")
        if not furnishedcheck:
            furnished1=response.xpath("//div/h1/text()[contains(.,'Eşyalı')]").get()
            if furnished1 and "eşyalı" in furnished1.lower():
                item_loader.add_value("furnished",True)
        
        desc="".join(response.xpath("//div[contains(@class,'styles_desc')]//p//text() | //div[contains(@class,'styles_desc')]/script/following-sibling::text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        balcony=response.xpath("//div[contains(@class,'styles_feature')][contains(.,'Balkon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking=response.xpath("//div[contains(@class,'styles_feature')][contains(.,'Otopark')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)  
        
        elevator=response.xpath("//div[contains(@class,'styles_feature')][contains(.,'Asansör')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)     
            
        images=[x for x in response.xpath("//div[contains(@class,'styles_carousel')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        landlord_name = response.xpath("//div[contains(@class,'styles_ownerDetail')]/h1/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())

        landlord_mail = response.xpath("//script[@id='__NEXT_DATA__']/text()").get()
        if landlord_mail:
            item_loader.add_value("landlord_email", landlord_mail.split('mail":"')[1].split('"')[0].strip())

        script_data = response.xpath("//script[@id='__NEXT_DATA__']/text()").get()
        if script_data:
            data = json.loads(script_data)
            if data:
                data = data["props"]["initialProps"]["pageProps"]["pageResponse"]

                if data and "location" in data:
                    location = data["location"]
                    if location and "city" in location:
                        item_loader.add_value("city", location["city"]["name"])

                if data and "owner" in data and "account" in data["owner"] and "phoneNumber" in data["owner"]["account"]:
                    landlord_phone = data["owner"]["account"]["phoneNumber"]
                    item_loader.add_value("landlord_phone", landlord_phone.strip("+"))
                latitude=data["location"]["coordinates"]["lat"]
                if latitude:
                    item_loader.add_value("latitude",latitude)
                longitude=data["location"]["coordinates"]["lon"]
                if longitude:
                    item_loader.add_value("longitude",longitude)


                    
                    
        yield item_loader.load_item()