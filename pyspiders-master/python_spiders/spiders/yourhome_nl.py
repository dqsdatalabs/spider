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
    name = 'yourhome_nl'  
    execution_type = 'testing' 
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.yourhome.nl/ajax.php?page=get_houses&iCurrentPage=4&iPage=1&iLang=2&sSort=date%3Adesc"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data = json.loads(response.body)
        for item in data:
            html_data = Selector(text=item["html"], type="html")
            follow_url = response.urljoin(html_data.xpath("//a/@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.yourhome.nl/ajax.php?page=get_houses&iCurrentPage=4&iPage={page}&iLang=2&sSort=date%3Adesc"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//div[@class='unit-70']//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return

        item_loader.add_value("external_source", "Yourhome_PySpider_netherlands")
        rented=response.xpath("//div[@class='card-head']/h4/text()").get()

        if rented and "verhuurd" in rented.lower(): 
            return 
        rented1=response.xpath("span[@class='label rented']/text()").get()
        if rented1 and "verhuurd" in rented1.lower():
            return


        external_id = response.url.strip('/').split('/')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//h1/text()").get()
        city = response.xpath("//h6/text()").get()
        if city:      
            item_loader.add_value("city", city.strip())
            if address: address += " " + city
        
        if address:
            item_loader.add_value("address", address.strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//h1/following-sibling::div[last()]//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//span[contains(.,'Oppervlak:')]/following-sibling::span[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split('.')[0].split(',')[0].strip())

        room_count = response.xpath("//span[contains(.,'Aantal slaapkamers:')]/following-sibling::span[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        rent = response.xpath("//div[@id='house-feature-card']//text()[contains(.,'€')]").get()
        if rent:
            rent = rent.split('€')[-1].split(',')[0].strip().replace('.', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//br/following-sibling::text()[contains(.,'Beschikbaar') or contains(.,'beschikbaar')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().split('beschikbaar')[1].split('per')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//text()[contains(.,'Waarborgsom') and contains(.,'-')]").get()
        if deposit:
            if '€' in deposit:
                item_loader.add_value("deposit", deposit.split('€')[-1].split(',')[0].strip().replace('.', ''))
            elif 'maand' in deposit:
                multiple = "".join(filter(str.isnumeric, deposit.strip()))
                if multiple:
                    item_loader.add_value("deposit", str(int(multiple) * int(rent)))
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='gallery']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('LatLng(')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip())
        
        energy_label = response.xpath("//text()[contains(.,'Energielabel') and contains(.,'-')]").get()
        if energy_label:
            if energy_label.split('Energielabel')[-1].strip().upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.split('Energielabel')[-1].strip().upper())

        utilities = response.xpath("//text()[contains(.,'Servicekosten') and contains(.,'-')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('Servicekosten')[1].split(',')[0].strip())

        pets_allowed = response.xpath("//text()[contains(.,'Geen huisdieren') or contains(.,'geen huisdieren') and contains(.,'-')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)
        
        parking = response.xpath("//text()[contains(.,'Gratis parkeren') or contains(.,'gratis parkeren') and contains(.,'-')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//text()[contains(.,'Balkon') or contains(.,'balkon') and contains(.,'-')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//span[contains(.,'Gemeubileerd:')]/following-sibling::span[1]/text()").get()
        if furnished:
            if furnished.strip().lower() == 'ja':
                item_loader.add_value("furnished", True)
            elif furnished.strip().lower() == 'nee':
                item_loader.add_value("furnished", False)

        elevator = response.xpath("//text()[contains(.,'Lift') or contains(.,'lift') and contains(.,'-')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//text()[contains(.,'Dakterras') or contains(.,'dakterras') and contains(.,'-')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        washing_machine = response.xpath("//text()[contains(.,'Wasmachine') or contains(.,'wasmachine') and contains(.,'-')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        item_loader.add_value("landlord_name", "Lody Bunschoten")
        item_loader.add_value("landlord_phone", "020 370 6020")
        item_loader.add_value("landlord_email", "info@yourhome.nl")
              
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None