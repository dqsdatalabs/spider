# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from scrapy import Request
import unicodedata


class MySpider(Spider):
    name = "atriumrealestate_be"
    handle_httpstatus_list = [403]
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'

    custom_settings = {
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "HTTPCACHE_ENABLED":False,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 3,
        "PROXY_ON" : True
    }


    headers = {           
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'tr,en;q=0.9',
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"

    }
    def start_requests(self):
        start_urls = [
            {"url": "http://www.atriumrealestate.be/index.php?ctypmandatmeta=l&action=list&reference=&categories%5B%5D=Appartement&chambre_min=&prix_max=", "property_type": "apartment"},
            {"url": "http://www.atriumrealestate.be/index.php?ctypmandatmeta=l&action=list&reference=&categories%5B%5D=Maison&chambre_min=&prix_max=", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             headers=self.headers,
                             meta={'property_type': url.get('property_type')})

    estate_list = []
    referer = "http://www.atriumrealestate.be/index.php?action=home"
    def parse(self, response):

        page = response.meta.get("page", 1)

        seen = False
        for item in response.xpath("//div[@class='picture']/a/@href").extract():
            follow_url = response.urljoin(item)
            self.estate_list.append(follow_url)
            seen = True

        if page == 1 or seen:
            url = f"http://www.atriumrealestate.be/index.php?page={page}&ctypmandatmeta=l&action=list&reference=&chambre_min=&prix_max=#toplist"
            yield Request(url, callback=self.parse, headers=self.headers, meta={'property_type': response.meta.get('property_type'), "page": page + 1})
        else:
            self.estate_list = list(set(self.estate_list))
            yield Request(
                self.referer,
                callback=self.jump,
                dont_filter=True,
            )
    
    def jump(self, response):
        current_index = response.meta.get("current_index", 0)
        yield Request(
            self.estate_list[current_index],
            callback=self.populate_item,
            dont_filter=True,
            meta={
                "property_type" : "apartment" if "appartement" in self.estate_list[current_index] else "house",
                "current_index" : current_index,
            }
        )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        is403 = True if response.status == 403 else False

        if not is403:
            item_loader.add_value("external_source", "Atriumrealestate_PySpider_" + self.country + "_" + self.locale)
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("property_type", response.meta.get('property_type'))
            
            item_loader.add_xpath("title", "//title/text()")
            desc = "".join(
                response.xpath("normalize-space(//div[@id='desc']/p/text())").extract()
            )
            if desc:
                if "ascenseur" in desc:
                    item_loader.add_value("elevator", True)
                if "balcons" in desc:
                    item_loader.add_value("balcony", True)
                item_loader.add_value("description", desc)

            price = response.xpath(
                "//div[@id='textbox']/p/text()[contains(., '€')]"
            ).extract_first()
            if price:
                item_loader.add_value("rent", price.split("€")[0])
                item_loader.add_value("currency", "EUR")
            ref = response.xpath(
                "//div[@id='textbox']/p/text()[contains(., 'Réf')]"
            ).extract_first()
            if ref:
                ref = ref.split(":")[1]
                item_loader.add_value("external_id", ref)
            square = response.xpath(
                "//ul[@class='check_list']/li/text()[contains(., 'Surface habitable')]"
            ).extract_first()
            if square:
                square = square.split(":")[1]
                item_loader.add_value("square_meters", square.split("m²")[0])
            item_loader.add_xpath("property_type", "//tr[th[.='Category']]/td")
            room = response.xpath("//ul[@class='check_list']/li/text()[contains(., 'Chambre')][not(contains(.,':'))]").extract_first()
            if room:
                room = room.split(" ")[0]
                item_loader.add_value("room_count", room)
            elif not room:
                room = response.xpath("//div[@id='desc']/div[@class='headline']/h2//text()[contains(.,'STUDIO')]").extract_first()
                if room:
                    item_loader.add_value("room_count", "1")                
                else:
                    room = response.xpath("substring-before(//ul[@class='check_list']/li[contains(., 'Chambre')][last()]/text(),':')").extract_first()
                    if room:
                        room = room.split("Chambre ")[1]
                        if room.isdigit():
                            item_loader.add_value("room_count", room.strip())          

            
            bath_room = response.xpath("//ul[@class='check_list']/li/text()[contains(., 'de bains')]").extract_first()
            if bath_room:
                bath_room = bath_room.split(" ")[0]
                if bath_room.isdigit():
                    item_loader.add_value("bathroom_count", bath_room)

            item_loader.add_xpath("available_date", "//tr[th[.='Availability']]/td")

            a = response.xpath(
                "//div[@id='desc']/div[@class='headline']/h3/text()"
            ).get()
            if a:
                property_type = a.split("-")[1].strip()
                if "non meublée" in a:
                    item_loader.add_value("furnished", False)
                elif "meublée" in a:
                    item_loader.add_value("furnished", True)
                try:
                    city = a.split(property_type)[1].strip().split(" ")[1]
                except:
                    city = None

                item_loader.add_value("city", city)

            # item_loader.add_xpath("utilities", "//tr[th[.='Charges (€) (amount)']]/td")
            utilities = response.xpath(
                "substring-after(//h4[contains(.,'Charges')]/following-sibling::ul[1]/li[contains(.,'Total:') and not(contains(.,'charges')) and not(contains(.,'tout')) and not(contains(.,'Total:  '))]/text(), 'Total: ')"
            ).extract_first()
            if utilities:
                u = utilities.split(" ")[0]
                if len(u) >= 2:
                    item_loader.add_value("utilities", u)
            else:
                utilities = response.xpath("substring-after(//ul[@class='check_list']/li[contains(.,'Charges: ')], 'Charges: ')").extract_first()
                if utilities:
                    item_loader.add_value("utilities", utilities)


            item_loader.add_xpath("floor", "//tr[th[.='Floor']]/td/text()")
            addres = response.xpath("//div[@id='page-title']//h2/span/text()").get()
            if addres:
                item_loader.add_value("address", addres.split(" -")[1].strip())
        
            images = [
                response.urljoin(x)
                for x in response.xpath(
                    "//div[@id='sliderx']//ul[@class='slides']/li/img/@src"
                ).extract()
            ]
            if images:
                item_loader.add_value("images", images)

        
            item_loader.add_value("landlord_phone", "025361340")
        
            item_loader.add_value("landlord_email", "info@atriumrealestate.be")

            item_loader.add_value("landlord_name", "Atrium Real Estate")

            parking = response.xpath(
                "//ul[@class='check_list']/li/text()[contains(., 'Parking') or contains(., 'Garage')]"
            ).extract_first()
            if parking:
                item_loader.add_value("parking", True)

            energy_lbl = response.xpath("substring-after(//ul[@class='check_list']/li/text()[contains(., 'Prestation énergétique')],':')").extract_first()
            if energy_lbl:
                energy_label = energy_lbl.strip().split(" ")[0]
                if energy_label and not energy_label.isdigit():
                    item_loader.add_value("energy_label", energy_label)
            terrace = response.xpath("//ul[@class='check_list']/li/text()[contains(., 'Terrasse')]").extract_first()
            if terrace:
                item_loader.add_value("terrace", True)
            elevator = response.xpath("//ul[@class='check_list']/li/text()[contains(., 'Ascenceur')]").extract_first()
            if elevator:
                item_loader.add_value("elevator", True)
            
            balcony = response.xpath("//ul[@class='check_list']/li/text()[contains(., 'Balcon')]").extract_first()
            if balcony:
                item_loader.add_value("balcony", True)
            dishwasher = response.xpath(
                "//ul[@class='check_list']/li/text()[contains(., 'Lave vaisselle')]"
            ).extract_first()
            if dishwasher:
                item_loader.add_value("dishwasher", True)

            washing_machine  = response.xpath(
                "//ul[@class='check_list']/li//text()[contains(., 'Lave linge')]"
            ).extract_first()
            if washing_machine :
                item_loader.add_value("washing_machine", True)

            elevator = response.xpath(
                "//ul[@class='check_list']/li/text()[contains(., 'Ascenseur')]"
            ).extract_first()
            if elevator:
                item_loader.add_value("elevator", True)
            
            empty ="".join(response.xpath("//h2//span//text()[.!=' - '] | //div[@id='page-title']/h2//text()").extract())
            if empty.strip() =="-":
                return
            yield item_loader.load_item()

        current_index = response.meta["current_index"]
        if current_index + 1 < len(self.estate_list) and not is403:
            yield Request(
                self.referer,
                callback=self.jump,
                dont_filter=True,
                meta={
                    "current_index" : current_index + 1,
                }
            )
        elif is403:
            yield Request(
                self.referer,
                callback=self.jump,
                dont_filter=True,
                meta={
                    "current_index" : current_index,
                }
            )
