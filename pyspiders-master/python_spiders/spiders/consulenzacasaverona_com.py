import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class ConsulenzacasaveronaComSpider(scrapy.Spider):
    name = 'consulenzacasaverona_com'
    allowed_domains = ['consulenzacasaverona.com']
    start_urls = [
        'https://www.consulenzacasaverona.com/IT/5-Residenziali/20-Affitto/-/-/-/pagina-1.html?cerca=top&home=true']
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#property-list-container > div"):
            url = appartment.css(
                "div.title.clearfix > h3 > a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

     # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            'body > section.post-wrapper-top.dm-shadow.clearfix > div > div:nth-child(2) > h2::text').get()

        city = response.css(
            '#content > div.property_wrapper.boxes.clearfix > div:nth-child(4) > small:nth-child(1)::text').get()

        rent = response.css(
            '#content > div.property_wrapper.boxes.clearfix > div:nth-child(4) > small:nth-child(3)::text').get().split('€')[1].strip()

        feats_keys = response.css(
            '#content > div.property_wrapper.boxes.clearfix > div:nth-child(7) > p > strong::text').extract()

        feats_values = response.css(
            '#content > div.property_wrapper.boxes.clearfix > div:nth-child(7) > p::text').extract()

        external_id = None
        rooms = None
        space = None
        floor = None
        for i in range(len(feats_keys)):
            if "Riferimento" in feats_keys[i]:
                external_id = feats_values[(i*2)+1]
            elif "Piano" in feats_keys[i]:
                floor = feats_values[(i*2)+1]
            elif "Locali" in feats_keys[i]:
                rooms = feats_values[(i*2)+1]
            elif "Superficie" in feats_keys[i]:
                space = feats_values[(i*2)+1].split('m')[0].strip()

        description = response.css(
            "#content > div.property_wrapper.boxes.clearfix > div.property_desc.clearfix::text").extract()\

        furnished = None
        try:
            if "ARREDATO" in response.css('#content > div.property_wrapper.boxes.clearfix > div:nth-child(5) > b::text').get():
                furnished = True
        except:
            pass

        images = response.css(
            'li > img.img-thumbnail::attr(src)').extract()

        i = 0
        while i < len(images):
            if "/_small/" in images[i]:
                images.pop(i)
                i = i - 1
            i += 1

        if "terra" in floor.lower():
            floor = "1"

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(space))
        item_loader.add_value("room_count", rooms)
        # item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("floor", floor)
        item_loader.add_value("address", city)
        item_loader.add_value("city", city)
        # item_loader.add_value("zipcode", zipcode.strip())
        # item_loader.add_value("available_date", avaialble_date)
        # item_loader.add_value("parking", parking)
        item_loader.add_value("furnished", furnished)
        # item_loader.add_value("balcony", balcony)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "EUR")

        # LandLord Details
        item_loader.add_value("landlord_name", "Consulenza Casa")
        item_loader.add_value("landlord_phone", '0458000811')
        item_loader.add_value(
            "landlord_email", 'info@consulenzacasaverona.com')

        yield item_loader.load_item()
