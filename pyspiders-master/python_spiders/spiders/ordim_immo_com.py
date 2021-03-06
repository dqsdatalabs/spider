# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json  

class MySpider(Spider): 
    name = 'ordim_immo_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "OrdimImmo_PySpider_france"
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }

    headers = {
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
    }

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.ordim-immo.com/catalog/advanced_search_result.php?action=update_search&search_id=1710495531956512&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MAX=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_34_MAX=&C_33_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://www.ordim-immo.com/catalog/advanced_search_result.php?action=update_search&search_id=1710495531956512&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MAX=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_34_MAX=&C_33_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords="
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1

        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    headers=self.headers,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING

    def parse(self, response):
        for item in response.xpath("//div[@class='cell-product']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

        total_page = response.xpath("//@data-total").get()
        if total_page:
            for i in range(2, int(total_page)):
                url = "https://www.ordim-immo.com/catalog/advanced_search_result.php"
                payload = f'aa_afunc=call&aa_sfunc=get_products_search_ajax&aa_cfunc=get_scroll_products_callback&aa_sfunc_args%5B%5D=%7B%22type_page%22%3A%22carto%22%2C%22infinite%22%3Atrue%2C%22sort%22%3A%22%22%2C%22page%22%3A{i}%2C%22nb_rows_per_page%22%3A6%2C%22search_id%22%3A1710495531956512%2C%22C_28_search%22%3A%22EGAL%22%2C%22C_28_type%22%3A%22UNIQUE%22%2C%22C_28%22%3A%22Location%22%2C%22C_27_search%22%3A%22EGAL%22%2C%22C_27_type%22%3A%22TEXT%22%2C%22C_27%22%3A%222%22%2C%22C_65_search%22%3A%22CONTIENT%22%2C%22C_65_type%22%3A%22TEXT%22%2C%22C_65%22%3A%22%22%2C%22C_30_MAX%22%3A%22%22%2C%22C_34_MIN%22%3A%22%22%2C%22C_34_search%22%3A%22COMPRIS%22%2C%22C_34_type%22%3A%22NUMBER%22%2C%22C_30_MIN%22%3A%22%22%2C%22C_30_search%22%3A%22COMPRIS%22%2C%22C_30_type%22%3A%22NUMBER%22%2C%22C_34_MAX%22%3A%22%22%2C%22C_33_MAX%22%3A%22%22%2C%22C_38_MAX%22%3A%22%22%2C%22C_36_MIN%22%3A%22%22%2C%22C_36_search%22%3A%22COMPRIS%22%2C%22C_36_type%22%3A%22NUMBER%22%2C%22C_36_MAX%22%3A%22%22%2C%22keywords%22%3A%22%22%7D'

                headers = {
                    'Connection': 'keep-alive',
                    'Pragma': 'no-cache',
                    'Cache-Control': 'no-cache',
                    'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
                    'sec-ch-ua-mobile': '?0',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
                    'sec-ch-ua-platform': '"Windows"',
                    'Content-type': 'application/x-www-form-urlencoded',
                    'Accept': '*/*',
                    'Origin': 'https://www.ordim-immo.com',
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Dest': 'empty',
                    'Accept-Language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
                }

                yield Request(
                    url,
                    dont_filter=True,
                    body=payload,
                    headers=headers,
                    method="POST",
                    callback=self.parse,
                    meta={
                        "property_type": response.meta.get('property_type'),
                    }
                )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        dontallow=response.xpath("//div[.='Type de transaction']/following-sibling::div/b/text()").get()
        if dontallow and "vendre" in dontallow:
            return 

        external_id = response.xpath(
            "//div[@class='product-model' and contains(.,'R??f??rence')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(
                "R??f??rence ")[1].replace("\t", ""))

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        description = response.xpath(
            "//div[@class='products-description']//text()").get()
        if description:
            item_loader.add_value("description", description)

        rent = response.xpath("//div[@class='prix loyer']//span[@class='alur_loyer_price']//text()").get()
        if rent:
            price= rent.split("???")[0].strip().lower().split("loyer")[1].replace("\xa0","").strip()
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        utilities=response.xpath("//div[.='Provision sur charges']/following-sibling::div/b/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("EUR")[0])
        available_date=response.xpath("//div[.='Disponibilit??']/following-sibling::div/b/text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)

        deposit = response.xpath("//div[.='D??p??t de Garantie']/following-sibling::div/b/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("EUR")[0])

        address = response.xpath(
            "//span[@class='ville-title']//text()").get()
        if address:
            item_loader.add_value("address", address)
            city = address.split(" ")[0]
            item_loader.add_value("city", city)
            zipcode = address.split(" ")[1]
            item_loader.add_value("zipcode", zipcode)

        bathroom_count = response.xpath(
            "//div[@class='bulle']//span[@class='value']//text()[contains(.,'bain')]").get()
        if bathroom_count:
            item_loader.add_value(
                "bathroom_count", bathroom_count.split("Salle de bain")[0])

        room_count = response.xpath(
            "//div[@class='bulle']//span[@class='value']//text()[contains(.,'pi??ce')]").get()
        if room_count:
            room_count = room_count.split("pi??ce")[0]
            item_loader.add_value("room_count", room_count)

        energy_label = response.xpath(
            "//div[@class='bulle']//div[@class='picto bold'][1]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        square_meters = response.xpath(
            "//div[@class='bulle']//span[@class='value']//text()[contains(.,'m??')]").get()
        if square_meters:
            square_meters = square_meters.split("m??")[0]
            item_loader.add_value("square_meters", square_meters)

        images = [response.urljoin(x) for x in response.xpath(
            "//div[@id='slider_product_large']//div//a//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        landlord_phone =response.xpath("//a[@class='contact_nego_tel']/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            landlord_phone =response.xpath("//a[@class='btn btn-secondary']/@title").get()
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone)
            
        item_loader.add_value("landlord_name", "Ordim Immobilier")

        yield item_loader.load_item()
