# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json, re
import dateparser

class MySpider(Spider):
    name = 'toulousecotepavee_stephaneplazaimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Toulousecotepavee_Stephaneplazaimmobilier_PySpider_france'
    custom_settings = {
        "PROXY_ON": True,
        "HTTPCACHE_ENABLED": False
    }
    
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
        "cookie":" __cfduid=d4ad5c339524d48bbf3590799e206b5b01611581298; _ga=GA1.2.858508417.1611581322; _pk_ses.194.f089=1; _pk_id.1.f089=397c1c78a7158eb4.1611903265.1.1611903265.1611903265.; _pk_ses.1.f089=1; tarteaucitron=!googleadwordsconversion=wait!gtag=wait!mautic=wait!facebook=wait!linkedin=wait!twitter=wait; _pk_id.194.f089=088c0c7d4c0dcf15.1611902008.1.1611903283.1611902008.; XSRF-TOKEN=eyJpdiI6IkE0MWMydkQxSEZLXC82VUtlNlhrcWp3PT0iLCJ2YWx1ZSI6Im1EeGZGdmI4UTRzcDhMbW1vaWRqalc3ME80R1ZUUzViaHNtXC9lMERpSmVlT25VZ0E1S21yaUdJaGMwNDJyV3V4IiwibWFjIjoiOTMwMDUwOGI5MDc1YTgyODdiYjA0NmJiNjQ2MmRmMzk2ODEzMWQ5MWI5MTNkZjQ1ZWU4YzE3NDE2NmNmY2E0ZCJ9; stephane_plaza_immobilier_session=eyJpdiI6IlR4N1Zib2o2N2ZJaWlIS3l1Rjh1aHc9PSIsInZhbHVlIjoiNVZneXlKWk5ETTVFbzBFblFJUUVTaFRLMTNteDY5SlpHN0wwZFwvMGlGZ295SWFRS3Rhb1BDdjM1azI4SDJaRlIiLCJtYWMiOiI3NzBjODc2MTAxMmI0MWEzZDM1NTNkY2Y2ODBiNGEwY2VlNDUxZTU2NzI5MDYzYTdmZmU4ZjIzNWRlNDdhMjIzIn0%3D",
        "referer": "https://toulousecotepavee.stephaneplazaimmobilier.com/immobilier-acheter?target=rent&agency_id=269&page=1",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36",
    }
    def start_requests(self):

        #apt_url = "https://toulousecotepavee.stephaneplazaimmobilier.com/search/rent?target=rent&type[]=1&agency_id=269&idagency=156187"
        apt_url = "https://toulousecotepavee.stephaneplazaimmobilier.com/location/appartement"
        #hs_url = "https://toulousecotepavee.stephaneplazaimmobilier.com/search/rent?target=rent&type[]=2&agency_id=269&idagency=156187"
        hs_url = "https://toulousecotepavee.stephaneplazaimmobilier.com/location/maison"

        yield Request(apt_url, headers=self.headers, callback=self.parse, meta={'property_type': "apartment"})     
        yield Request(hs_url, headers=self.headers,callback=self.parse, meta={'property_type': "house"}) 

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='content-wrap']//a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("(//title//text())[1]").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        external_id = response.xpath("//div[@class='subhead-left breadcrumbs']/ul/li[contains(.,'Référence')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('Référence')[1].strip())
        
        rent = response.xpath("//div[@class='de-row-elements-element-row']/label[contains(.,'Loyer charges')]/following-sibling::span//text()").get()
        if rent:  
            price = rent.split('€')[0].replace(" ","")                
            item_loader.add_value("rent_string",price)
        
        deposit = response.xpath("//div[@class='de-row-elements-element-row']/label[contains(.,'Depot')]/following-sibling::span//text()").get()
        if deposit:  
            deposit = deposit.split('€')[0].replace(" ","")                
            item_loader.add_value("deposit", deposit)
        utilities = response.xpath("//div[@class='de-row-elements-element-row']/label[contains(.,'copropriété')]/following-sibling::span//text()").get()
        if utilities:  
            utilities = utilities.split('€')[0].replace(" ","")                
            item_loader.add_value("utilities", utilities)
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("(//div[@class='de-row-elements-element-row']/label[contains(.,'Surface')]/following-sibling::span//text())[1]").get()
        if square_meters:  
            square_meters = square_meters.split('m')[0].strip()               
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//div[@class='de-row-elements-element-row']/label[contains(.,'Chambres')]/following-sibling::span//text()").get()
        if room_count:  
            room_count = room_count.strip()               
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[@class='de-row-elements-element-row']/label[contains(.,'de bain')]/following-sibling::span//text()").get()
        if bathroom_count:  
            bathroom_count = bathroom_count.strip()               
            item_loader.add_value("bathroom_count", bathroom_count)

        zipcode = response.xpath("//div[@class='de-row-elements-element-row']/label[contains(.,'postal')]/following-sibling::span//text()").get()
        if zipcode:  
            zipcode = zipcode.replace(",","")                  
            item_loader.add_value("zipcode",zipcode.strip().replace(" ",""))
            city = "Toulouse"
            address = zipcode + " " + city
            item_loader.add_value("city", city)
            item_loader.add_value("address", address.strip().replace(" ","").replace("\n"," "))

        available_date = response.xpath("//div[@class='de-row-elements-element-row']/label[contains(.,'Date de')]/following-sibling::span//text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                        str(available_date), date_formats=["%d/%m/%Y"]
                    )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        
        description = "".join(response.xpath("(//div[@id='description']//p//text())[1]").getall())
        if description:
            item_loader.add_value("description", description.strip().replace("\n",""))
        floor = response.xpath("//div[@class='de-row-elements-element-row']/label[contains(.,'Etage X/X')]/following-sibling::span//text()").get()
        if floor:                
            item_loader.add_value("floor", floor)
        images = [response.urljoin(x) for x in response.xpath("//meta[@property='og:image']/@content").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("//div[@class='de-row-elements-element-row']/label[contains(.,'Meublé')]/following-sibling::span//text()").get()
        if furnished:
            if furnished is not 'Non':
                item_loader.add_value("furnished", True)
        elevator = response.xpath("//div[@class='de-row-elements-element-row']/label[contains(.,'Ascenseur')]/following-sibling::span//text()").get()
        if elevator:                
            item_loader.add_value("elevator", True)
        balcony = response.xpath("//div[@class='de-row-elements-element-row']/label[contains(.,'balcons')]/following-sibling::span//text()").get()
        
        if balcony:
            if int(balcony) > 0:                
                item_loader.add_value("balcony", True)

        item_loader.add_value("landlord_name", "Stéphane Plaza Immobilier Toulouse Côte Pavée")
        item_loader.add_value("landlord_phone", "05 61 24 85 54")

        yield item_loader.load_item()
