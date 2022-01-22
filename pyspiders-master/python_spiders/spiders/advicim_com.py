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

class MySpider(Spider):
    name = 'advicim_com'
    execution_type='testing'
    country='france'
    locale='fr'

    post_url = "http://www.advicim.com/wp-admin/admin-ajax.php"
    current_index = 0
    other_prop = ["maison", "studio", "duplex"]
    other_prop_type = ["house", "studio", "apartment"]
    duplicate_check = []
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }
    def start_requests(self):
        formdata = {
            "action": "ac_search_estate",
            "data": "category=rent&type=appartement&city=&rooms=&price=200%7C2500&area=15%7C1065&order=date&pagination=1&nonce=0b4b02c416",
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.parse,
            dont_filter=True,
            headers=self.headers,
            formdata=formdata,
            meta={
                "base_query":"category=rent&type=appartement&city=&rooms=&price=200%7C2500&area=15%7C1065&order=date&pagination={}&nonce=0b4b02c416",
                "property_type":"apartment",
            }
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data = json.loads(response.body)
        if data and "html" in data:
            sel = Selector(text=data["html"], type="html")
            for item in sel.xpath("//a[contains(@class,'estate-card flex-item')]/@href").getall():
                follow_url = response.urljoin(item)
                yield Request(follow_url, dont_filter=True, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
                seen = True
        if page == 2 or seen:
            base_query = response.meta["base_query"]
            formdata = {
                "action": "ac_search_estate",
                "data": base_query.format(page),
            }
            yield FormRequest(self.post_url, dont_filter=True, formdata=formdata, callback=self.parse, meta={"property_type":response.meta["property_type"], "base_query":base_query, "page":page+1})
        elif self.current_index < len(self.other_prop):
            formdata = {
                "action": "ac_search_estate",
                "data": f"category=rent&type={self.other_prop[self.current_index]}&city=&rooms=&price=200%7C2500&area=15%7C1065&order=date&pagination=1&nonce=0b4b02c416",
            }
            base_query = "category=rent&type=" + self.other_prop[self.current_index] + "&city=&rooms=&price=200%7C2500&area=15%7C1065&order=date&pagination={}&nonce=0b4b02c416",
            yield FormRequest(
                url=self.post_url,
                callback=self.parse,
                dont_filter=True,
                headers=self.headers,
                formdata=formdata,
                meta={
                    "base_query":str(base_query),
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        duplicate = True if response.url in self.duplicate_check else self.duplicate_check.append(response.url)
        if duplicate:
            return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Advicim_PySpider_france")
        if  "-" in response.url:
            external_id = response.url.split("-")[-1].split("/")[0]
            item_loader.add_value("external_id",external_id)
        else:
            external_id = response.url.split("/")[-2]
            item_loader.add_value("external_id",external_id)

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//p[contains(.,'Localisation')]/parent::div//div//text()").get()
        if address:
            city = address.split(",")[0]
            zipcode = address.split(",")[-2]
            item_loader.add_value("address",address)
            item_loader.add_value("city",city)
            item_loader.add_value("zipcode",zipcode.strip())
        
        rent = response.xpath("//div[contains(@class,'price')]//span[contains(@class,'text')]//text()").get()
        if rent:
            rent = rent.split("€")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        utilities_deposit = " ".join(response.xpath("//p[contains(.,'Informations financières')]/parent::div//text()").getall())
        if utilities_deposit:
            utilities_deposit = re.sub('\s{2,}', ' ', utilities_deposit.strip())
            if "Charges" in utilities_deposit:
                utilities = utilities_deposit.split("|")[1].split(":")[1].split("€")[0].strip()
                item_loader.add_value("utilities", utilities)
            if "garantie" in utilities_deposit:
                deposit = utilities_deposit.split(":")[-1].split("€")[0].strip()
                item_loader.add_value("deposit", deposit)

        square_meters = response.xpath("//span[contains(.,'m²')]//text()").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//span[contains(.,'chambre')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = "".join(response.xpath("//li[contains(.,'pièces')]//text()").getall())
            if room_count:
                room_count = room_count.split(":")[1].strip()
                item_loader.add_value("room_count", room_count)

        parking = "".join(response.xpath("//li[contains(.,'Parking')]//text()").getall())
        if parking:
            item_loader.add_value("parking", True)
        
        floor = "".join(response.xpath("//li[contains(.,'Étage')]//text()").getall())
        if floor:
            floor = floor.split(":")[1].strip().replace("ème","").replace("er","")
            item_loader.add_value("floor", floor)
        
        desc = " ".join(response.xpath("//p[contains(.,'Description du bien')]/parent::div//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        from datetime import datetime
        import dateparser
        if desc and "disponible " in desc.lower():
            available_date = desc.lower().split("disponible ")[1]
            match = re.search(r'(\d+/\d+/\d+)', available_date)
            match2 = re.search(r'(\d+.\d+.\d+)', available_date)
            available = ""
            if match:
                newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)
            elif match2:
                try:
                    newformat = dateparser.parse(match2.group(1), languages=['en']).strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", newformat)
                except:pass
            elif "immédiatement" in available_date or "immediatement" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            elif "/" in available_date:
                available = available_date.replace("le","").replace(":","").strip().split(" ")[0]
            elif "du " in available_date:
                available = available_date.split("du ")[1].split(".")[0]
            if available:
                date_parsed = dateparser.parse(available, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            
        utilities_deposit = " ".join(response.xpath("//p[contains(.,'Informations financières')]/parent::div//text()").getall())
        if utilities_deposit:
            utilities_deposit = re.sub('\s{2,}', ' ', utilities_deposit.strip())
            if "Charges" in utilities_deposit:
                utilities = utilities_deposit.split("|")[1].split(":")[1].split("€")[0].strip()
                item_loader.add_value("utilities", utilities)
            if "garantie" in utilities_deposit:
                deposit = utilities_deposit.split(":")[-1].split("€")[0].strip()
                item_loader.add_value("deposit", deposit)
        
        energy_label = response.xpath("//div[contains(@class,'energy')]//img//@src").get()
        if energy_label:
            energy_label = energy_label.split("=")[-1]
            item_loader.add_value("energy_label", energy_label)

        images = [x for x in response.xpath("//section[contains(@id,'estate-gallery')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Advicim")
        item_loader.add_value("landlord_phone", "02 38 54 00 49")

        yield item_loader.load_item()