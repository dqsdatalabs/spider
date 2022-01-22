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
    name = 'properly_space'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Properly_PySpider_united_kingdom"
    start_urls = ["https://www.properly.space/find-a-property/?action=search&type=property&page=1&sort=price-highest&per-page=12&view=list&tenure=lettings&location=&radius=3&resale-price-min=0&lettings-price-min=0&resale-price-max=999999999999&lettings-price-max=999999999999&bedrooms-min=0&bedrooms-max=0&include=true&property-type%5B%5D=apartment%2Cflat%2Cground-flat%2Cground-maisonette%2Cmaisonette%2Cpenthouse"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='link-cover']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.properly.space/find-a-property/?action=search&type=property&page={page}&sort=price-highest&per-page=12&view=list&tenure=lettings&location=&radius=3&resale-price-min=0&lettings-price-min=0&resale-price-max=999999999999&lettings-price-max=999999999999&bedrooms-min=0&bedrooms-max=0&include=true&property-type[]=apartment,flat,ground-flat,ground-maisonette,maisonette,penthouse"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1}
            )    
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        desc = "".join(response.xpath("//h2[contains(@class,'text-secondary') and contains(.,'description')]/following-sibling::p/text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))

        item_loader.add_value("external_source", self.external_source)

        external_id = response.url.split('-')[-1].split('/')[0]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//div[contains(@class,'property-details')]//p[@class='lead']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip())
            if '-london-' in response.url:
                item_loader.add_value("city", 'London')
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//h2[contains(.,'Full property description')]/following-sibling::p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//li[contains(.,'sq ft') or contains(.,'sqft')]/text()").get()
        if square_meters:
            square_meters=square_meters.replace(",","")
            item_loader.add_value("square_meters", str(int(float(square_meters.split('sq')[0].strip()) * 0.09290304)))

        room_count = response.xpath("//small[contains(.,'bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.lower().split('bedroom')[0].strip())
        
        bathroom_count = response.xpath("//small[contains(.,'bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.lower().split('bathroom')[0].strip())

        rent = response.xpath("//h4[contains(@class,'text-secondary')]/text()").get()
        if rent:
            rent = rent.split('£')[-1].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'GBP')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//p[contains(.,'Available')]/text()[contains(.,'Available')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split('Available')[1].split('*')[0].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-slider']//a/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//a[contains(@href,'property-floorplan')]/img/@data-src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//iframe[@id='locrate-map']/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude.strip())

        longitude = response.xpath("//iframe[@id='locrate-map']/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude.strip())
        
        floor = response.xpath("//li[contains(text(),'Floor')]/text()").get()
        if floor:
            item_loader.add_value("floor", "".join(filter(str.isnumeric, floor.strip())))

        pets_allowed = response.xpath("//li[contains(text(),'Pet friendly')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        
        parking = response.xpath("//li[contains(text(),'Parking') or contains(text(),'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(text(),'Balcony') or contains(text(),'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[text()='Furnished']").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(text(),'Lift') or contains(text(),'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//li[contains(text(),'Terrace') or contains(text(),'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        swimming_pool = response.xpath("//li[contains(text(),'Swimming Pool') or contains(text(),'swimming pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        item_loader.add_value("landlord_name", "Properly")
        item_loader.add_value("landlord_phone", "0207 459 4400")
        item_loader.add_value("landlord_email", "info@properly.space")
      
        yield item_loader.load_item()

    
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None