# Author: Sounak Ghosh
import scrapy
import js2xml
from ..loaders import ListingLoader
from ..items import ListingItem
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date
import re,json
import requests

class QuotesSpider(scrapy.Spider):
    name = 'agenceibox_PySpider_france_fr'
    allowed_domains = ['www.agenceibox.com']
    start_urls = ['www.agenceibox.com']
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    custom_settings = {
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }
  

    def start_requests(self):
        start_urls = [{"url":"https://www.agenceibox.com/location/1"}]

        for urls in start_urls:
            yield scrapy.Request(
                url=urls.get('url'),
                callback=self.parse,
                meta = {"url":urls.get("url")}
                )

    def parse(self, response, **kwargs):

        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'property-listing-v3__item ')]"):
            f_url = response.urljoin(item.xpath(".//a[contains(.,'Voir')]/@href").get())
            yield scrapy.Request(f_url, callback=self.get_property_details)
            seen = True
            
        if page==2 or seen:
            f_url = f"https://www.agenceibox.com/location/{page}"
            yield scrapy.Request(f_url, callback=self.parse, meta={"page": page+1})

       
    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        description = " ".join(response.xpath("//div[contains(@class,'about__text-block')]//p//text()").getall())
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description)) 
        # else:
        #     if get_p_type_string(response.url):
        #         item_loader.add_value("property_type", get_p_type_string(description))
        #     else: return
        if True:
            titleborder=" ".join(response.xpath("//ol[@class='breadcrumb__items']//li//text()").getall())
            if get_p_type_string(titleborder): 
                item_loader.add_value("property_type", get_p_type_string(titleborder))
            else:return  
        else:
            titletext=" ".join(response.xpath("//div[@class='title-subtitle']/h1/span/text()").getall())
            if get_p_type_string(titletext):
                item_loader.add_value("property_type", get_p_type_string(titletext))
            else:return

 

        item_loader.add_value("external_source", "agenceibox_PySpider_france_fr")
        
        title = response.xpath("//h1[contains(@class,'subtitle')]//text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//span[contains(.,'Ville')]/following-sibling::span/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        zipcode = response.xpath("//span[contains(.,'Code')]/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        square_meters = response.xpath("//span[contains(.,'habitable')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.replace(",",".").split(" ")[0]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//span[contains(.,'pièces')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else: 
            room_count = response.xpath("//span[contains(.,'pièce')]/following-sibling::span/text()").get()
            if room_count: item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//span[contains(.,'salle')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//span[contains(.,'Loyer')]/following-sibling::span/text()").get()
        if rent:
            rent = rent.replace(" ","").split("€")[0].replace(",",".").strip()
            item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "EUR")
        
        floor = response.xpath("//span[contains(.,'Etage')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        deposit = response.xpath("//span[contains(.,'garantie')]/following-sibling::span/text()").get()
        if deposit:
            deposit = deposit.split(" ")[0].replace(" ","").replace(",",".")
            if "non" not in deposit.lower():
                item_loader.add_value("deposit", int(float(deposit)))

        utilities = response.xpath("//span[contains(.,'Charge')]/following-sibling::span/text()").get()
        if utilities:
            utilities = utilities.split(" ")[0].replace(" ","").replace(",",".")
            item_loader.add_value("utilities", int(float(utilities)))
        
        furnished = response.xpath("//span[contains(.,'Meublé')]/following-sibling::span/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//span[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//span[contains(.,'Balcon')]/following-sibling::span/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            elif "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//span[contains(.,'Terrasse')]/following-sibling::span/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            elif "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
        
        latitude = response.xpath("//div/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//div/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        external_id = response.xpath("substring-after(//div[contains(@class,'info-id') and contains(.,'Réf')]/text(),':')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        if description:
            item_loader.add_value("description", description)
        
        images = [x for x in response.xpath("//div[contains(@class,'img__swiper-slide')]/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "IBox agencies")
        item_loader.add_value("landlord_phone", "04.22.14.55.10")
        item_loader.add_value("landlord_email", "gestion@agenceibox.com")
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None 