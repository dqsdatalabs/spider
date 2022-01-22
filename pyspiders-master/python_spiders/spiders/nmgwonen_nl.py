# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser


class MySpider(Spider):
    name = 'nmgwonen_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1
    
    headers = {
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
        "origin": "https://nmgwonen.nl"
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://nmgwonen.nl/huur/#q1Yqz8_LzEsvqSxIVbJSSiwoSCwqSc1NzStR0lEqSExPLQaKZpSWFinVAgA", "property_type": "apartment"},
            {"url": "https://nmgwonen.nl/huur/#q1Yqz8_LzEsvqSxIVbICcvLzMkozi5V0lAoS01OLgUIZpaVFSrUA", "property_type": "house"},
            {"url": "https://nmgwonen.nl/huur/#q1Yqz8_LzEsvqSxIVbJSMtLNz0tJLdI11M1OLIDIKOkoFSSmpxYDZTNKS4uUagE", "property_type": "house"},
            {"url": "https://nmgwonen.nl/huur/#q1Yqz8_LzEsvqSxIVbJSSs3MS4EIKOkoFSSmpxYDBTNKS4uUagE", "property_type": "house"},
            {"url": "https://nmgwonen.nl/huur/#q1Yqz8_LzEsvqSxIVbJSSk8tTs5IzE7NSUmFiCvpKBUkAkWBchmlpUVKtQA", "property_type": "house"},
            {"url": "https://nmgwonen.nl/huur/#q1Yqz8_LzEsvqSxIVbJSyshPzYYIKOkoFSSmpxaDBEtLi5RqAQ", "property_type": "house"},
            {"url": "https://nmgwonen.nl/huur/#q1Yqz8_LzEsvqSxIVbJSKiktLk7Ngwgp6SgVJKanFgOFM0pLi5RqAQ", "property_type": "house"},
            {"url": "https://nmgwonen.nl/huur/#q1Yqz8_LzEsvqSxIVbJSKivKzCouSUzMS0mFiCvpKBUkpqcWA-UySkuLlGoB", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "base_url":url.get('url')})

    # 1. FOLLOWING
    def parse(self, response):
        prop_type = ["tussenwoning", "appartement", "hoekwoning", "woonhuis"]
        for p_type in prop_type:
            data = {
                "__live": "1",
                "__templates[]": "search",
                "__templates[]": "loop",
                "__maps": "paged",
                "adres_plaats_postcode": "",
                "woningtype": f"{p_type}",
                "huurprijs[min]": "",
                "huurprijs[max]": "",
                "adres": "",
                "beschikbaar": "",
                "ligging[]": "",
                "status[]": "",
                "woonoppervlakte[min]": "",
                "woonoppervlakte[max]": "",
                "perceel[min]": "",
                "perceel[max]": "",
                "aantalkamers[min]": "",
                "aantalkamers[max]": "",
           }
           
            url = f"https://nmgwonen.nl/huur/page/1/"
            yield FormRequest(
                url,
                formdata=data,
                dont_filter=True,
                headers=self.headers,
                callback=self.jump,
                meta={"prop_type": p_type, "property_type": response.meta.get("property_type")}
            )
            
    def jump(self, response):
        
        jresp = json.loads(response.body)
        prop_type = response.meta.get("prop_type")
        
        page = response.meta.get('page', 2)
        
        for item in jresp["maps"]:
            content = item["template"]
            sel = Selector(text=content, type="html")
            url = sel.xpath("//a[@class='house__overlay']/@href").extract_first()
            yield Request(url, callback=self.populate_item, meta={"property_type": response.meta.get("property_type")})
        
        if jresp:
            data = {
                "__live": "1",
                "__templates[]": "search",
                "__templates[]": "loop",
                "__maps": "paged",
                "adres_plaats_postcode": "",
                "woningtype": f"{prop_type}",
                "huurprijs[min]": "",
                "huurprijs[max]": "",
                "adres": "",
                "beschikbaar": "",
                "ligging[]": "",
                "status[]": "",
                "woonoppervlakte[min]": "",
                "woonoppervlakte[max]": "",
                "perceel[min]": "",
                "perceel[max]": "",
                "aantalkamers[min]": "",
                "aantalkamers[max]": "",
          }
           
            url = f"https://nmgwonen.nl/huur/page/{page}/"
            yield FormRequest(
                url,
                formdata=data,
                headers=self.headers,
                callback=self.jump,
                meta={"prop_type": prop_type, "page":page+1, "property_type": response.meta.get("property_type")}
            )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Nmgwonen_PySpider_" + self.country + "_" + self.locale)
        status = response.xpath("//div/dl/dt[contains(.,'Status')]//following-sibling::dd[1]/text()").extract_first()
        if status and ("onder optie" in status.lower() or "verhuurd " in status.lower()):
            return
        prop_type = response.meta.get("property_type")
        item_loader.add_value("property_type", prop_type)
        item_loader.add_xpath("title", "normalize-space(//h1)")
        item_loader.add_value("external_link", response.url)

        price = response.xpath("//dl[dt[. ='Huurprijs']]/dd[1]/text()").extract_first()
        if price:
            item_loader.add_value("rent", price.split("€")[1].split(",")[0])
            item_loader.add_value("currency", "EUR")

        utilities = response.xpath("//li[@class='woning__list-item']/span[contains(.,'€')]").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[1].split(",")[0])

        square = response.xpath("//dl[dt[. ='Woonoppervlakte']]/dd[1]/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m")[0])

        images = [response.urljoin(x)for x in response.xpath("//div[@class='woning__slider woning-slider']//figure/a/@href").extract()]
        if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x)for x in response.xpath("//div[contains(@class,'floorplan')]/div/img/@data-lazy-src").getall()]
        if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images)

        floor = response.xpath("//dt[contains(.,'Aantal verdiepingen')]/following-sibling::dd[1]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        item_loader.add_xpath("room_count","//dl[dt[. ='Aantal kamers']]/dd[1]/text()")

        available_date = response.xpath("//div/dl/dt[contains(.,'Aanvaarding datum')]//following-sibling::dd/text()").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        desc = "".join(response.xpath("//div[@class='woning__description js--has-toggler']/div/p/text()").extract())
        item_loader.add_value("description", desc)

        if desc:

            if "waarborgsom:" in desc.lower():
                deposit = desc.lower().split("waarborgsom:")[1].split(",")[0].strip()
                if deposit and "€" in deposit:
                    item_loader.add_value("deposit", deposit.strip())

            if 'huisdieren zijn niet toegestaan' in desc.lower():
                item_loader.add_value("pets_allowed", False)
            if 'parkeren' in desc.lower() or 'garage' in desc.lower():
                item_loader.add_value("parking", True)
            if 'balkon' in desc.lower():
                item_loader.add_value("balcony", True)
            if 'terras' in desc.lower():
                item_loader.add_value("terrace", True)
            if 'zwembad' in desc.lower():
                item_loader.add_value("swimming_pool", True)
            if 'vaatwasser' in desc.lower():
                item_loader.add_value("dishwasher", True)
            if 'wasmachine' in desc.lower():
                item_loader.add_value("washing_machine", True)

        energy_label = response.xpath("//div/dl/dt[contains(.,'Energielabel')]/following-sibling::dd[1]/text()").get()
        if energy_label:
            if energy_label.strip() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label)

        terrace = "".join(response.xpath("//div[@class='woning__feature']/dl/dt[contains(.,'Voorzieningen wonen')]/following-sibling::dd/text()").extract()).strip()
        if terrace:
            if "Lift" in terrace or "Elavator" in terrace :
                item_loader.add_value("elevator", True)

        address = "".join(response.xpath("normalize-space(//h1)").extract())
        item_loader.add_value("address",address)
        item_loader.add_value("city", address.split(" ")[-1])

        latlng = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split('"latitude":')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latlng.split('"longitude":')[1].split(',')[0].strip())


        item_loader.add_value("landlord_phone", "088 200 70 00")
        item_loader.add_value("landlord_email", "info@nmg.nl")
        item_loader.add_value("landlord_name", "NmgWonen")

        yield item_loader.load_item()