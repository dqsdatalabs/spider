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
    name = 'immoregion_fr' 
    execution_type = 'testing'
    country = 'france'
    locale = 'fr' 
    external_source="Immoregion_PySpider_france"
    # LEVEL 1
    def start_requests(self):
        yield Request("https://www.immoregion.fr/location?recent_published=15", 
                    callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2) 
        seen = False

        for item in response.xpath("//article[@itemprop='itemListElement']"):
            seen = True
            follow_url = response.urljoin(item.xpath("./link/@href").get())
            property_type = item.xpath(".//h3/a/text()[1]").get()
            if get_p_type_string(property_type): 
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})

        
        if page == 2 or seen: 
            url = f"https://www.immoregion.fr/location?recent_published=15&page={page}"
            yield Request(url, callback=self.parse, meta={"page": page + 1})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", self.external_source)
        dontallow=response.xpath("//h1//text()").get()
        if dontallow and "erreur 404" in dontallow.lower():
            return 
        title=response.xpath("//title/text()").get()
        if title:
            title=title.replace("immoRegion","").split("|")[0].rstrip()
            item_loader.add_value("title", title)
        item_loader.add_xpath("address", "substring-after(//h1[@class='KeyInfoBlockStyle__PdpTitle-sc-1o1h56e-2 hWEtva']/text(),'à ')")
        item_loader.add_xpath("city", "substring-after(//h1[@class='KeyInfoBlockStyle__PdpTitle-sc-1o1h56e-2 hWEtva']/text(),'à ')")
        item_loader.add_xpath("external_id", "//div[@class='content-body']/section/h1/span/text()[last()]")
        item_loader.add_xpath("bathroom_count", "//div[contains(@class,'KeyInfoBlockStyle__LogoContainer-sc-1o1h56e-11')]/ul/li/i[@class='icon-bath']/following-sibling::div//text()")
        item_loader.add_xpath("energy_label", "//span[@class='energy fr-dpe-e']/text()")
        item_loader.add_xpath("deposit", "//li[div[.='Dépôt de garantie']]/div[2]//text()")
        item_loader.add_xpath("floor", "//li[div[.='Etage du bien']]/div[2]//text()")

        room_count = response.xpath("//div[contains(@class,'KeyInfoBlockStyle__LogoContainer-sc-1o1h56e-11')]/ul/li/i[@class='icon-room']/following-sibling::div//text()").get()
        if room_count:
           item_loader.add_value("room_count", room_count.strip()) 
        else:
            room_count = response.xpath("//div[contains(@class,'KeyInfoBlockStyle__LogoContainer-sc-1o1h56e-11')]/ul/li/i[@class='icon-bed']/following-sibling::div//text()").get()
            if room_count:
                item_loader.add_xpath("room_count", room_count.strip()) 

        address = response.xpath("//div[@class='block-localisation-address']/div/text()").extract_first()
        if address:
           item_loader.add_value("address", address.replace("-","").strip()) 
 
        square_meters = response.xpath("//div[contains(@class,'KeyInfoBlockStyle__LogoContainer-sc-1o1h56e-11')]/ul/li/i[@class='icon-surface']/following-sibling::div//text()").extract_first()
        if square_meters:
           item_loader.add_value("square_meters", square_meters.split(" ")[0].strip()) 

        # available_date=response.xpath("//li[div[.='Disponibilité']]/div[2]//text()").get()
        # if available_date:
        #     date2 =  available_date.strip()
        #     date_parsed = dateparser.parse(
        #         date2, date_formats=["%m-%d-%Y"]
        #     )
        #     date3 = date_parsed.strftime("%Y-%m-%d")
        #     item_loader.add_value("available_date", date3)

        images = [response.urljoin(x)for x in response.xpath("//picture//img/@src").getall()]
        if images:
                item_loader.add_value("images", images)

        desc = " ".join(response.xpath("//div[@class='collapsed']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip().replace("immoRegion",""))

        rent = response.xpath("//div[contains(@class,'KeyInfoBlockStyle__Price-sc-1o1h56e-5')]/h2/text()").extract_first()
        if rent:
           price = rent.replace(" ","").replace("\u202f","").replace("\xa0","").replace("€","").strip()
           if int(price) > 99999:
               price = re.search(r"loyer.+:\s(\d+)€", desc.lower())
               if price: 
                   price = price.group(1)
           if price != "0":
            item_loader.add_value("rent", price) 
        item_loader.add_value("currency", "EUR")

        utilities = response.xpath("//ul[contains(@class,'sc-1o1h56e-8-KeyInfoBlockStyle__Informations-hsqygf')]/li/div[contains(.,'Charges mensuelles')]/span/text()").extract_first()
        if utilities:
            uti = utilities.replace("€","").strip()
            if uti !="0":
                item_loader.add_value("utilities", uti.strip()) 
        
        deposit = response.xpath("//ul[contains(@class,'sc-1o1h56e-8-KeyInfoBlockStyle__Informations-hsqygf')]/li/div[contains(.,'Charges mensuelles')]/span/text()").extract_first()
        if deposit:
            dep = deposit.replace("€","").strip()
            if dep !="0":
                item_loader.add_value("deposit", dep.strip()) 

        furnished = response.xpath("//li[div[.='Meublé']]/div[2]//text()").extract_first()
        if furnished:
            if "oui" in furnished.lower():
                item_loader.add_value("furnished",True)
            else:
                item_loader.add_value("furnished",False)

        parking = response.xpath("//i[contains(@class,'car')]//following-sibling::div//text()").extract_first()
        if parking:
            if parking:
                item_loader.add_value("parking",True)

        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//li/div[contains(.,'Disponibilité')]//following-sibling::div//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        latitude_longitude = response.xpath('//script[contains(.,\'"lat"\')]//text()').get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"lat":"')[1].split('"')[0]
            longitude = latitude_longitude.split('lon":"')[1].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        item_loader.add_value("landlord_name", "Immo Region")
        
        landlord_phone = response.xpath("//a[contains(@href,'tel:')]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)


        data_script = response.xpath("//script[contains(.,'window.__INITIAL_STATE__')]/text()").get()

        # with
        if data_script:
            data = data_script.split("__INITIAL_STATE__ =")[-1].strip().strip(";")

            items = json.loads(data)["detail"]["media"]["items"]
            images = []
            for item in items:
                url = "https://i1.static.athome.eu/images/annonces2/image_" + item["uri"]
                images.append(url)
            item_loader.add_value("images",images)
            item_loader.add_value("external_images_count",len(images))

            phone = json.loads(data)["detail"]["publisher"].get("phone1")
            if phone:
                item_loader.add_value("landlord_phone",phone)

            mail = json.loads(data)["detail"]["publisher"].get("email")
            if mail:
                item_loader.add_value("landlord_email",mail)




   



        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    else:
        return None

