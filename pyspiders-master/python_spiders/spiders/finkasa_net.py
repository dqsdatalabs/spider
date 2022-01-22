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
from  geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'finkasa_net'
    execution_type='testing'
    country='spain'
    locale='es'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.finkasa.net/es/browser/s1?quick_search%5Btof%5D=2&quick_search%5BpropertyTypes%5D=%7B%22g1%22%3A%7B%22tp%22%3A%5B1%2C2%2C4%2C8%2C16%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Atrue%7D%2C%22g2%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22g4%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22g8%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22g16%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22text%22%3A%22Pisos%22%7D&quick_search%5BfullLoc%5D=%7B%22p%22%3A%5B%5D%2C%22c%22%3A%5B%5D%2C%22d%22%3A%5B%5D%2C%22q%22%3A%5B%5D%2C%22text%22%3A%22%22%7D&quick_search%5B_token%5D=X1-PGaOoCkgJSjTnAmxMjXKwPj4xkWw9cz6V7W6cHQM",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "http://www.finkasa.net/es/browser/s1?quick_search%5Btof%5D=2&quick_search%5BpropertyTypes%5D=%7B%22g1%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22g2%22%3A%7B%22tp%22%3A%5B32%2C64%2C128%2C256%2C16384%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Atrue%7D%2C%22g4%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22g8%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22g16%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22text%22%3A%22Casas%22%7D&quick_search%5BfullLoc%5D=%7B%22p%22%3A%5B%5D%2C%22c%22%3A%5B%5D%2C%22d%22%3A%5B%5D%2C%22q%22%3A%5B%5D%2C%22text%22%3A%22%22%7D&quick_search%5B_token%5D=X1-PGaOoCkgJSjTnAmxMjXKwPj4xkWw9cz6V7W6cHQM",

                ],
                "property_type" : "house"
            },
            

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//img[@class='img-responsive']/../@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@class='ie_nextPage']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Finkasa_PySpider_"+ self.country + "_" + self.locale)

        studio = "".join(response.xpath("//ul/li/strong[. ='Tipo:']/following-sibling::text()").extract()).strip()
        if "Estudio" in studio:
            item_loader.add_value("property_type", "studio")
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)

        title=response.xpath("//span[@class='detail-title']/text()").get()
        item_loader.add_value("title", title)

        latitude_longitude = response.xpath("//img[@id='div-button-google-street-view']/../@href").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('viewpoint=')[-1].split(',')[0].strip()
            longitude = latitude_longitude.split('viewpoint=')[-1].split(',')[1].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        address = response.xpath("//span[@class='detail-title']/text()").get()
        if address:
            address = address.split("alquiler en ")[1].split(",")[0].strip()
            item_loader.add_value("address", address)

        external_id=response.xpath("//div[@class='pgl-detail']/div/div/ul/li[contains(.,'Ref')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        room_count="".join(response.xpath("//div[@class='pgl-detail']/div/div/ul/li[contains(.,'Dormitorio')]/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        
        square_meters="".join(response.xpath("//div[@class='pgl-detail']/div/div/ul/li/span[contains(.,'m')]/text()").getall())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())

        bathroom = response.xpath("substring-before(//ul[@class='list-icon check-square']/li[contains(.,'Baño')],'Baño')").extract_first()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        rent=response.xpath("//div[@class='panel-body']/div/div/div/h2//text()[.!='A consultar']").get()
        if rent:
            item_loader.add_value("rent_string", rent)
        else:
            item_loader.add_value("currency", "EUR")
        
        deposit="".join(response.xpath("//div[@class='panel-body']/div/div/div/ul/li[contains(.,'Fianza')]//text()").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip())
        
        utilities="".join(response.xpath("//div[@class='panel-body']/div/div/div/ul/li[contains(.,'Honorarios')]//text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
                  
        desc="".join(response.xpath("//div[@class='col-sm-8']/p//text()").getall()).replace('\n', '').replace('\xa0', '')
        if desc:
            item_loader.add_value("description", desc)
        
        furnished=response.xpath("//ul/li[contains(.,'Amueblado')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        terrace=response.xpath("//div[@class='panel-body']/div/div/ul/li[contains(.,'Terraza')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        elevator=response.xpath("//div[@class='panel-body']/div/div/ul/li[contains(.,'Ascensor')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        if "BALCÓN" in desc:
            item_loader.add_value("balcony", True)

        if "NO MASCOTA" in desc:
            item_loader.add_value("balcony", False)

        washing_machine=response.xpath("//ul/li[contains(.,'Lavadora')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        swimming_pool=response.xpath("//ul/li[contains(.,'Piscina')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
            
        images=[response.urljoin(x) for x in response.xpath("//div/ul/li/img/@src").getall()]
        for image in images:
            item_loader.add_value("images", image)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name","FINKASA")
        item_loader.add_value("landlord_phone","34 923 215 444")
        item_loader.add_value("landlord_email","finkasainmobiliaria@gmail.com")
        
        
        yield item_loader.load_item()