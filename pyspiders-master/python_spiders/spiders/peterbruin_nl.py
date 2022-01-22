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
from word2number import w2n

class MySpider(Spider):
    name = 'peterbruin_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    
    headers = {
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
        "origin": "https://www.peterbruin.nl"
    }
    
    def start_requests(self):

        data = {
            "sorteer": "Desc~Prijs",
            "prijs": "0,999999999",
            "prefilter": "Huuraanbod",
            "pagenum":"0",
            "pagerows":"12",
       }
       
        yield FormRequest(
            "https://www.peterbruin.nl/huizen/smartselect.aspx",
            dont_filter=True,
            formdata=data,
            headers=self.headers,
            callback=self.jump,
        )
    
    def jump(self, response):
        data = json.loads(response.body)
        
        ids = ""
        for item_id in data["AllMatches"]:
            
            ids += item_id + ","
        
        form_data = {
            "id": ids.rstrip(",")
        }
       
        yield FormRequest(
            "https://www.peterbruin.nl/huizen/smartelement.aspx",
            dont_filter=True,
            formdata=form_data,
            headers=self.headers,
            callback=self.parse,
        )
    
    # 1. FOLLOWING
    def parse(self, response):
       
        for follow_url in response.xpath(" //div[contains(@class,'object-element1')]"):
            f_url = follow_url.xpath(".//div[@class='object-adres']/a/@href").extract_first()
            prop = follow_url.xpath(".//div[@class='object-feature'][.//div[contains(.,'Soort')]]//div[contains(@class,'features-info')]/text()").extract_first()
            if prop:
                if "Appartement" in prop:
                    property_type = "apartment"
                elif "Hoekwoning" in prop or "Eengezinswoning" in prop or "woonhuis" in prop or "Tussenwoning" in prop or "Maisonnette" in prop or "Benedenwoning" in prop:
                    property_type = "house"
                else:
                    property_type = None
                yield Request(f_url, callback=self.populate_item, meta={"property_type": property_type})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//div[@class='status status-under-offer']//span//text()").get()
        if status and "Onder bod" in status:
            return
        
        item_loader.add_value("external_source", "Peterbruin_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//h1/span[@class='adres']/text()").extract_first()
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
      
        property_type = response.meta.get("property_type")
        if property_type:
            item_loader.add_value("property_type", property_type)


        zipcode = response.xpath("//div[contains(@class,'description-small')]/h3/following-sibling::text()[1]").get()
        if zipcode:
            zipcode = " ".join(zipcode.split(',')[-1].strip().split(' ')[:-1]).strip()
            if not zipcode.count(" ")>1 and not (zipcode.split(" ")[0].isalpha() or zipcode.isdigit()):
                zipcode = zipcode.replace("Amsterdam","").strip()
                if zipcode:
                    item_loader.add_value("zipcode", zipcode)

        price = response.xpath("//h2/text()").get()
        if price:
            item_loader.add_value("rent", price.split("€")[-1].strip().split(' ')[0].strip().replace('.', ''))
            item_loader.add_value("currency", "EUR")
        else:
            price = response.xpath("//br/following-sibling::text()[contains(.,'Total rental price')]").get()
            if price:
                item_loader.add_value("rent", price.split("€")[-1].split(',')[0].strip().replace('.', ''))
                item_loader.add_value("currency", "EUR")
        
        external_id = response.url.split('-')[-1].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)

        bathroom_count = response.xpath("//div[contains(text(),'Aantal badkamer')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        available_date = response.xpath("//br[contains(following-sibling::text(),'Available per')]/following-sibling::text()[1]").get()
        if available_date:
            available_date = available_date.split('per')[-1].split('.')[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%B/%Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        pets_allowed = response.xpath("//br[contains(following-sibling::text(),'pets')]/following-sibling::text()[1]").get()
        if pets_allowed and 'no pets allowed' in pets_allowed.lower():
            item_loader.add_value("pets_allowed", False)
        
        floor_plan_images = [x for x in response.xpath("//section[@id='object-a4']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        images = [response.urljoin(x)for x in response.xpath("//section[@id='object-photos']//img/@src").extract()]
        if images:
                item_loader.add_value("images", list(set(images)))
                item_loader.add_value("external_images_count", len(list(set(images))))

        desc = "".join(response.xpath("//div[@class='object-detail-description object-detail-description-small']/text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        item_loader.add_value("description", desc.strip())

        if desc:
            if 'balcony' in desc.lower() or 'balkon' in desc.lower():
                item_loader.add_value("balcony", True)
            if 'elevator' in desc.lower():
                item_loader.add_value("elevator", True)
            if 'terrace' in desc.lower():
                item_loader.add_value("terrace", True)
            if 'dishwasher' in desc.lower():
                item_loader.add_value("dishwasher", True)
            if 'washing machine' in desc.lower():
                item_loader.add_value("washing_machine", True)
            if 'floor' in desc.lower():
                try:
                    floor = w2n.word_to_num(desc.lower().split('floor')[0].strip().split(' ')[-1].strip().rstrip('st').rstrip('nd').rstrip('rd').rstrip('th'))
                    item_loader.add_value("floor", str(floor))
                except:
                    pass

        deposit = response.xpath("//div[div[. ='Waarborgsom']]/div[contains(@class,'features-info')]/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[1].strip())

        square = response.xpath("//div[div[. ='Gebruiksoppervlak wonen']]/div[contains(@class,'features-info')]/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m")[0])

        label = response.xpath("//div[contains(@class,'features-info')]/span/text()").get()
        if label:
            item_loader.add_value("energy_label", label.strip().replace("Klasse","").strip())

        images = [response.urljoin(x)for x in response.xpath("//div[@class='col-md-8']//div[@id='galleria']//a/@href").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_xpath("room_count","//div[div[. ='Totaal aantal kamers']]/div[contains(@class,'features-info')]/text()")

        parking = "".join(response.xpath("//div[@class='object-detail-description object-detail-description-small']//text()[contains(.,'parking')]").getall())
        if parking:
            item_loader.add_value("parking", True)

        terrace = "".join(response.xpath("//div[div[. ='Specifiek']]/div[contains(@class,'features-info')]/text()").extract()).strip()
        if terrace:
            item_loader.add_value("furnished", True)

        latlng = response.xpath("(//input[@id='ssInfoHuisID']/following-sibling::input//@value)[1]").get()
        if latlng:
            item_loader.add_value("latitude",latlng.split('~',2)[-1].split(',')[0])
            item_loader.add_value("longitude",latlng.split(',')[1].split('~',3)[0])

        address = "".join(response.xpath("//div[@class='object-adres mt-4']/h1/span/text()").extract())
        item_loader.add_value("address", address)

        city = response.xpath("//h1/span[@class='plaatsnaam']/text()").extract_first()

        if city:
            item_loader.add_value("city", city.split(",")[0])

        item_loader.add_value("landlord_phone", "020 6768022")
        item_loader.add_value("landlord_email", "info@peterbruin.nl")
        item_loader.add_value("landlord_name", "Peter Bruin")
        
        yield item_loader.load_item()